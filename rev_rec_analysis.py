import os
import re
import json
import glob
import argparse
from typing import Dict, Iterable, List, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

EMAIL_DOMAIN_RE = re.compile(r'@([^@>\s]+)$')

def extract_domain(email: str):
    """Extract domain from email address."""
    if not email:
        return None
    m = EMAIL_DOMAIN_RE.search(email.strip())
    return m.group(1).lower() if m else None

def read_file_content(path: str) -> Dict:
    """Load file into memory and return dictionary with path and content."""
    print(f"[INFO] Reading file: {path}")
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    return {'path': path, 'content': content}

def parse_json_objects(file_record: Dict) -> Iterable[Dict]:
    """
    Parse JSON objects from file:
    - If file contains a JSON array → return each element
    - If file contains JSON per line → parse line by line
    """
    content = file_record.get('content', '')
    source_path = file_record.get('path', '')
    project_name = os.path.splitext(os.path.basename(source_path))[0]
    objs = []

    try:
        parsed = json.loads(content)
        if isinstance(parsed, list):
            objs = parsed
        elif isinstance(parsed, dict):
            objs = [parsed]
    except Exception:
        # Fallback to JSON-per-line format
        objs = []
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                objs.append(json.loads(line))
            except Exception:
                continue

    for o in objs:
        o['__source_project'] = project_name
        yield o

def to_reviewer_records(change: Dict) -> Iterable[Tuple[str, Dict]]:
    """Extract reviewer records as key-value pairs, safely handling missing values."""
    reviewers = change.get('reviewers') or []
    ts = change.get('timestamp')
    for r in reviewers:
        account = r.get('accountId') or r.get('name') or r.get('email') or 'unknown'
        email = (r.get('email') or '').strip()
        name = (r.get('name') or '').strip()
        key = str(account)
        yield (key, {'accountId': r.get('accountId'), 'email': email, 'name': name, 'timestamp': ts})

def to_owner_record(change: Dict) -> Tuple[str, Dict]:
    """Extract change owner as key-value pair, safely handling missing values."""
    owner = change.get('owner') or {}
    account = owner.get('accountId') or owner.get('name') or owner.get('email') or 'unknown'
    email = (owner.get('email') or '').strip()
    name = (owner.get('name') or '').strip()
    key = str(account)
    return (key, {'accountId': owner.get('accountId'), 'email': email, 'name': name})

def to_file_records(change: Dict) -> Iterable[Tuple[Tuple[str,str], int]]:
    """Extract file modification info per subproject."""
    sub = change.get('subProject') or change.get('__source_project') or '<root>'
    fps = change.get('filePaths') or []
    for f in fps:
        loc = f.get('location') or f.get('path') or None
        if loc:
            yield ((sub, loc), 1)

def dict_to_json_line(d):
    """Convert dict to JSON line."""
    return json.dumps(d, ensure_ascii=False)

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default='SEAA2018_rev-rec_dataset/*.json',
                        help='Pattern for input JSON files.')
    parser.add_argument('--output', default='out/results', help='Output directory.')
    parser.add_argument('--top_n', type=int, default=20, help='Top N entries to select.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    input_pattern = known_args.input
    output_prefix = known_args.output
    top_n_value = known_args.top_n

    print("[INFO] Starting JSON Review Processing Pipeline")
    print(f"[INFO] Input pattern : {input_pattern}")
    print(f"[INFO] Output folder : {output_prefix}")
    print(f"[INFO] Top N         : {top_n_value}")

    file_list = glob.glob(input_pattern, recursive=True)
    if not file_list:
        print(f"[ERROR] No files found for pattern: {input_pattern}")
        print("Make sure path is correct.")
        return
    print(f"[INFO] Found {len(file_list)} input files.")

    os.makedirs(output_prefix, exist_ok=True)

    options = PipelineOptions(flags=pipeline_args, runner='DirectRunner')
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:

        print("[INFO] Loading and parsing JSON files...")
        files = (
            p
            | "CreateFileList" >> beam.Create(file_list)
            | "ReadFilesToDict" >> beam.Map(read_file_content)
        )

        reviews = files | "ParseJsonObjects" >> beam.FlatMap(parse_json_objects)

        # Most active reviewers
        print("[INFO] Computing most active reviewers...")
        reviewer_kv = (
            reviews
            | "ExtractReviewerRecords" >> beam.FlatMap(to_reviewer_records)
            | "ReviewerKeyToTuple" >> beam.Map(lambda kv: ((kv[0], kv[1].get('email') or '', kv[1].get('name') or ''), 1))
            | "CountReviewers" >> beam.CombinePerKey(sum)
            | "FormatReviewerSummary" >> beam.Map(lambda kv: {
                'id_or_key': kv[0][0],
                'email': kv[0][1],
                'name': kv[0][2],
                'count': kv[1]
            })
        )

        top_reviewers = (
            reviewer_kv
            | "ReviewersToList" >> beam.combiners.ToList()
            | "SelectTopReviewers" >> beam.Map(lambda arr: sorted(arr, key=lambda r: r['count'], reverse=True)[:top_n_value])
        )

        # Most active authors (owners)
        print("[INFO] Computing most active authors...")
        owners = (
            reviews
            | "ExtractOwner" >> beam.Map(to_owner_record)
            | "OwnerToTuple" >> beam.Map(lambda kv: ((kv[0], kv[1].get('email') or '', kv[1].get('name') or ''), 1))
            | "CountOwners" >> beam.CombinePerKey(sum)
            | "FormatOwnerSummary" >> beam.Map(lambda kv: {
                'id_or_key': kv[0][0],
                'email': kv[0][1],
                'name': kv[0][2],
                'count': kv[1]
            })
            | "OwnersToList" >> beam.combiners.ToList()
            | "TopOwners" >> beam.Map(lambda arr: sorted(arr, key=lambda o: o['count'], reverse=True)[:top_n_value])
        )

        # Most frequently modified files per subproject
        print("[INFO] Computing most modified files per subproject...")
        files_count = (
            reviews
            | "ExtractFileRecords" >> beam.FlatMap(to_file_records)
            | "CountFiles" >> beam.CombinePerKey(sum)
        )

        files_by_project = (
            files_count
            | "MapFileToProject" >> beam.Map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
            | "GroupFilesByProject" >> beam.GroupByKey()
            | "TopFilesPerProject" >> beam.Map(lambda kv: {
                'subProject': kv[0],
                'top_files': sorted(list(kv[1]), key=lambda x: x[1], reverse=True)[:top_n_value]
            })
        )

        # Reviewer groups by email domain
        print("[INFO] Computing reviewer groups by email domain...")
        reviewer_domains = (
            reviews
            | "ExtractReviewersForDomain" >> beam.FlatMap(to_reviewer_records)
            | "MapReviewerDomain" >> beam.Map(lambda kv: (extract_domain(kv[1].get('email') or ''), (kv[0], kv[1].get('email') or kv[1].get('name') or '')) )
            | "FilterDomainNotNone" >> beam.Filter(lambda kv: kv[0] is not None)
            | "GroupByDomain" >> beam.GroupByKey()
            | "FormatDomainGroups" >> beam.Map(lambda kv: {
                'domain': kv[0],
                'unique_reviewers_count': len(set([v[0] for v in kv[1]])),
                'reviewers': list({v[0]: v[1] for v in kv[1]}.items())
            })
        )

        print("[INFO] Writing output files...")

        (top_reviewers
         | "TopReviewersToJSON" >> beam.Map(dict_to_json_line)
         | "WriteTopReviewers" >> beam.io.WriteToText(os.path.join(output_prefix, "top_reviewers"),
                                                      shard_name_template='',
                                                      file_name_suffix='.json'))

        (owners
         | "OwnersToJSON" >> beam.Map(dict_to_json_line)
         | "WriteOwners" >> beam.io.WriteToText(os.path.join(output_prefix, "top_owners"),
                                                shard_name_template='',
                                                file_name_suffix='.json'))

        (files_by_project
         | "FilesByProjectToJSON" >> beam.Map(dict_to_json_line)
         | "WriteFilesByProject" >> beam.io.WriteToText(os.path.join(output_prefix, "files_by_project"),
                                                        shard_name_template='',
                                                        file_name_suffix='.json'))

        (reviewer_domains
         | "DomainsToJSON" >> beam.Map(dict_to_json_line)
         | "WriteDomains" >> beam.io.WriteToText(os.path.join(output_prefix, "domains"),
                                                 shard_name_template='',
                                                 file_name_suffix='.json'))

    print("[INFO] Processing completed successfully!")
    print(f"[INFO] Results saved to directory: {output_prefix}")

if __name__ == "__main__":
    run()

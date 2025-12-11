# Rev-Rec Source Code Reviews Analysis

This project processes the **Rev-Rec Source Code Reviews Dataset (SEAA2018)** and extracts insights about reviewers, owners, frequently changed files, and reviewer groups by organization.

The pipeline is implemented in **Python** using **Apache Beam** with the **DirectRunner**. It processes JSON files per subproject and outputs aggregated results in the `out/results` folder.

---

## Project Structure
/app

├─ rev_rec_analysis.py # Main Apache Beam pipeline

├─ test_rev_rec_analysis.py # Unit tests for data processing functions and Beam transforms

├─ SEAA2018_rev-rec_dataset/ # JSON dataset files

├─ requirements.txt

├─ Dockerfile

└─ README.md


**Input:** JSON files per subproject in `SEAA2018_rev-rec_dataset/`.  

**Output:** Aggregated results in `out/results/`, e.g.:

| File                                   | Purpose |
|----------------------------------------|---------|
| `top_reviewers.json`                   | Most active reviewers |
| `top_owners.json`                      | Most active change owners |
| `most_changed_files.json`              | Most frequently changed files per subproject |
| `reviewers_by_domain.json`             | Groups of reviewers by email domain (organization) |

---

## Features

1. **Process JSON code review files**  
   Reads multiple JSON files per project and extracts relevant fields.

2. **Top active reviewers and owners**  
   Aggregates counts of reviews per reviewer and changes per owner.

3. **Most frequently changed files per subproject**  
   Aggregates file paths to identify hotspots in each subproject.

4. **Reviewer groups by organization**  
   Groups reviewers based on email domains to identify organizational clusters.

5. **Unit tests included**  
   Ensures correctness of data transformations and Beam pipeline operations.

---

## Running Locally

Install dependencies:

```
pip install -r requirements.txt
```

Run code:

```
python rev_rec_analysis.py --input "SEAA2018_rev-rec_dataset/*.json" --output out/results --top_n 30
```
Run test:

```
python -m unittest test_rev_rec_analysis.py  
```

## Running with Docker

Build the Docker image:

```
docker build -t revrec-beam .
```

Run the pipeline:

```
docker run --rm -v SEAA2018_rev-rec_dataset:/app/SEAA2018_rev-rec_dataset revrec-beam python rev_rec_analysis.py --input "SEAA2018_rev-rec_dataset/*.json" --output out/results --top_n 30
```

Run unit tests inside Docker:

```
docker run --rm -v SEAA2018_rev-rec_dataset:/app/SEAA2018_rev-rec_dataset revrec-beam python -m unittest test_rev_rec_analysis.py  
```

## Dataset

The dataset comes from Figshare: [Rev-Rec Source - Code Reviews Dataset SEAA2018](https://figshare.com/articles/dataset/Rev-rec_Source_-_Code_Reviews_Dataset_SEAA2018_/6462380)

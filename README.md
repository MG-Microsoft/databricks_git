# Running Notebooks in Production

While notebooks are powerful documents for exploring and sharing data, they are often considered "not production ready" due to difficulties integrating with established software engineering or DevOps best practices.  To help overcome these difficulties and confusion, this project is a _minimal example_ of what running a 'productionized' notebook could look like.  We achieve this by incorporating four pillars of sound software engineering:

* Version control
* Modular code
* Unit and integration tests
* CI/CD

## Pre-requisites
* [Personal access token for the Databricks REST API](https://docs.databricks.com/dev-tools/api/latest/authentication.html)
* [Set up the Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)

## Project structure

```.
├──.github/workflows
│   └── databricks_gha.yml
├── README.md
├── conf
│   └── compute_spec.json
├── covid_analysis
│   └── transforms.py
├── covid_eda.py
├── requirements.txt
└── tests
    ├── run_unit_tests.py
    └── transforms_test.py
```

This `README.md` lives in the root directory alongside our project dependencies in `requirements.txt`, and the Databricks notebook `covid_eda.py`. In addition, we have a few directories containing Databricks compute configurations (`/conf`), our python modules (`/covid_analysis`), tests (`/tests`), and our GitHub Actions configuration (`.github/workflows/databricks_gha.yml`) for CI/CD. 

## Development workflow

1. **Version control**

Whether you have an existing notebook or are starting from scratch, start by getting your existing work into version control (like GitHub).  This repo can be synced to Databricks and your local IDE.  

2. **Modular code**

Modules are best developed in python files using your local IDE, with periodic syncs of code to Databricks via version control to test out functionality on live data in the notebook.  For a very tight dev loop experience, you can use [`dbfs-sync`](https://github.com/databricks/dbfs-sync) to instantly sync code between your local IDE and Databricks (on the same branch) without having to leverage your version control provider as an intermediary.  

3. **Testing**

When modules are working well, you can begin to develop unit tests in your IDE.  In this project we use `pytest` with fixtures for mocking dataframes as input.  For integration tests, we use for a widget in our notebook to tell Databricks whether the notebook is being run as a test or as a production job.

4. **CI/CD**

Next, we add a GitHub Actions workflow that will run our integration test whenever a pull request is made on the repo.  

5. **Deploy to production**

Finally, we configure a job using Databricks Workflows to run on the `main` branch of our repo.  This gives us confidence that production jobs will always run on code that has been tested.

## Reproducibility
To ensure reproducibility between development, testing and production environments we save the Databricks compute specification under `/conf/compute_spec.json`.  Library dependencies are captured via `requirements.txt` and are installed at runtime.

## Step by step instructions

Coming soon... :) 

### ODSC Semantic Engineering
Hello, thank you for attending my session at the Open Data Science Conference. In this codebase you will find the necessary code to run a basic semantic recommendation system. 

### Quick Start
1. Create python virtual environment
2. Install requirements from `requirements.txt`
    - `$ pip install -r requirements.txt`
3. Activate airflow in two separate terminals running the virtual environment. 
    1. `airflow webserver`
    2. `airflow scheduler`
4. Edit the `airflow.cfg` file (usually in the Airflow folder found in your home directory) so that `dags_folder` points to the `dags` folder from this repo. Repeat for `plugins_folder` and `plugins`. 
5. Install [GraphDB free version](https://www.ontotext.com/products/graphdb/download/)
6. Launch GraphDB and create a new repository called `ODSC_Demo`. 
7. Rename `.env.example` to `.env`. 
8. You can find Airflow at http://0.0.0.0/8080
9. From the Airflow interface, you can run `taxonomy_pipeline` followed by `content_pipeline` to prepare the data. 
10. Once the data is loaded into GraphDB (you can check at http://localhost:7200), you can run `workbench.py` to run the recommendation algorithm and see results. 
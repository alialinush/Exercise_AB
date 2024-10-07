Description: this application takes 3 inputs from the user (paths for dataset_one.csv, dataset_two.csv, dataset_three.csv) and returns the
requested outputs in separate directories (department_breakdown, it_data and marketing_address_info). Additionaly, the application includes
3 tests that were conducted on loading the data, transforming the data (not null/null scenario) and exporting the data.

Instructions:
- create a virtual environment using python3 -m venv venv
- activate the virtual environment using source venv/bin/activate
- install the dependencies from requirements.txt using pip install -r requirements.txt
- set the PYTHONPATH to include your directory (e.g. export PYTHONPATH="/Users/alinabaciu/IdeaProjects/Test:$PYTHONPATH"), to let Python
locate the necessary modules
- run the application via python -m src.main (and pass the 3 inputs)
- run the tests via python -m tests.test   

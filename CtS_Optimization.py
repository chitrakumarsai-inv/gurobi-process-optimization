from snowflake.snowpark import Session
import os
import dotenv

# Read config parser .ini file with your connection information

Path = "../.env"
dotenv.load_dotenv(Path)

# SSO - KochID
connection_parms = {
    "account": os.getenv('account'),
    "user": os.getenv('email'),
    "authenticator": os.getenv('authenticator'),
    "database": os.getenv('database'),
    "schema": os.getenv('schema'),
    "warehouse": os.getenv('warehouse'),"role": os.getenv('role')
} 

# Create Snowpark session
session = Session.builder.configs(connection_parms).create()

# Define the optimization function
def optimize_cts(session: Session) -> str:
    import time
    import gurobipy as gp  # Import inside function
    from gurobipy import GRB  # Import inside function
    import pandas as pd

    start = time.time()

    # Read data from Snowflake
    sheets = {
        'Production': session.table("PRODUCTION").to_pandas(),
        'Warehouses': session.table("WAREHOUSES").to_pandas(),
        'Transportation': session.table("TRANSPORTATION").to_pandas(),
    }

    for key in sheets:
        sheets[key].fillna(0, inplace=True)

    # Define Gurobi License Key
    keys = {
        "GURO_PAR_ISVNAME": "Infor",
        "GURO_PAR_ISVAPPNAME": "Infor",
        "GURO_PAR_ISVEXPIRATION": 20270930,
        "GURO_PAR_ISVKEY": "G9FKJ8IG"
    }

    # Run optimization using Gurobi
    with gp.Env(params=keys) as env:
        with gp.Model(env=env) as model:
            Production = model.addVars(sheets['Production']['ID'].tolist(), vtype='C', name='Production')
            Beg_Inv = model.addVars(sheets['Warehouses']['ID'].tolist(), vtype='C', name='Beg_Inv')
            Lane = model.addVars(sheets['Transportation']['ID'].tolist(), vtype='C', name='Lane')

            # Define Example Constraints
            for i in range(len(sheets['Production'])):
                model.addConstr(Production[sheets['Production']['ID'].iloc[i]] <= 1000)

            # Define Objective Function
            obj_function = sum(Production[i] * sheets['Production']['Variable Cost'].iloc[i] for i in sheets['Production']['ID'].tolist())
            model.setObjective(obj_function, gp.GRB.MAXIMIZE)

            # Optimize
            model.optimize()

            # Store results
            sheets['Production']['Results'] = sheets['Production']['ID'].apply(lambda i: round(Production[i].X, 2))
            sheets['Warehouses']['Results'] = sheets['Warehouses']['ID'].apply(lambda i: round(Beg_Inv[i].X, 2))
            sheets['Transportation']['Results'] = sheets['Transportation']['ID'].apply(lambda i: round(Lane[i].X, 2))

    # Write results back to Snowflake tables
    session.write_pandas(sheets['Production'], "PRODUCTION_RESULTS", auto_create_table=True, overwrite=True)
    session.write_pandas(sheets['Warehouses'], "WAREHOUSE_RESULTS", auto_create_table=True, overwrite=True)
    session.write_pandas(sheets['Transportation'], "TRANSPORTATION_RESULTS", auto_create_table=True, overwrite=True)

    run_time = time.time() - start
    return f"Optimization completed in {round(run_time, 2)} seconds!"

# Register the stored procedure in Snowflake using Snowpark
session.sproc.register(
    func=optimize_cts,
    name="optimize_cts",
    replace=True,
    return_type="string",
    language="python",
    runtime_version="3.10",
    packages=["numpy", "pandas", "snowflake-snowpark-python"],
    artifact_repository="your_repository_path",  # Update with your actual Snowflake repo
    artifact_repository_packages=["gurobipy"]  # ✅ Explicitly add gurobipy
)

print("Stored Procedure 'optimize_cts()' registered successfully!")

# Close session
session.close()

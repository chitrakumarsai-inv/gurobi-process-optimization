import time
start = time.time()
from gurobipy import Model, GRB, Var, LinExpr, Env
import gurobipy as gp
from gurobipy import GRB
import numpy as np
import pandas as pd 
import openpyxl as oxl

#set file names
Input_File = pd.ExcelFile('CtS Optimization Input.xlsx')
Output_File = 'CtS Optimization Output.xlsx'

# Read Excel File
sheets = pd.read_excel(Input_File, sheet_name=None)
for i in sheets:
    sheets[i].fillna(0, inplace=True)
#Initial Period Calculation and Historical variable setting
sdf_Periods = sheets['Periods'].sort_values(by='Order')
initial_Period = sdf_Periods['Period'][0]
Historicals = sheets['Settings'][sheets['Settings']['Setting'] == 'Historicals']['Value'].tolist()[0]

#Functions to expand input datasets where input values where defined in groups, an "!" signifies that each individual item within the group abides by the capacities and fixed costs 
#while no "!" means the sum of the group abides by the capacities and fixed costs
def check(st1):
    return st1.startswith('!')

def expand_hierarchy_with_exclamation(df, hierarchy_df, column_name):
    position = 0
    while position < len(df):
        value = str(df.at[position, column_name])
        if check(value) and value[1:] in hierarchy_df['Group'].tolist():
            sub_groups = hierarchy_df[hierarchy_df['Group'] == value[1:]]['Sub Group'].tolist()
            for sub_group in sub_groups:
                new_row = df.loc[position].copy()
                new_row[column_name] = '!' + str(sub_group)
                df = df._append(new_row, ignore_index=True)
            df.drop(index=position, inplace=True)
            df.reset_index(drop=True, inplace=True)
        else:
            position += 1
    for i in range(len(df)):
        value = str(df.at[i, column_name])
        if check(value):
            df.at[i, column_name] = value[1:]
    return df

def expand_hierarchy(df, hierarchy_df, column_name):
    original_column = column_name
    df = df.rename(columns={original_column: f'Original {original_column}'})
    column_name = f'Original {original_column}'
    while df[column_name].isin(hierarchy_df['Group']).any():
        merged_df = df.merge(hierarchy_df, left_on=column_name, right_on='Group', how='left', suffixes=('', '_Sub'))
        merged_df[f'{original_column}'] = merged_df['Sub Group'].where(merged_df['Sub Group'].notna(), merged_df[column_name])
        df = merged_df.drop(columns=['Group', 'Sub Group'])
        column_name = f'{original_column}'
    df[f'{original_column}'] = df[column_name]
    return df
#expanding necessary dataframes by their group items
Grp_Production = expand_hierarchy_with_exclamation(sheets['Production'], sheets['Product Groups'], 'Product')
Grp_Production = expand_hierarchy_with_exclamation(Grp_Production, sheets['Period Groups'], 'Period')
Grp_Production = expand_hierarchy_with_exclamation(Grp_Production, sheets['Location Groups'], 'Location')
final_columns = [col for col in Grp_Production.columns if not col.startswith('Sub')]
Grp_Production = Grp_Production[final_columns]

Grp_Warehouses = expand_hierarchy_with_exclamation(sheets['Warehouses'], sheets['Product Groups'], 'Product')
Grp_Warehouses = expand_hierarchy_with_exclamation(Grp_Warehouses, sheets['Period Groups'], 'Period')
Grp_Warehouses = expand_hierarchy_with_exclamation(Grp_Warehouses, sheets['Location Groups'], 'Location')
final_columns = [col for col in Grp_Warehouses.columns if not col.startswith('Sub')]
Grp_Warehouses = Grp_Warehouses[final_columns]

Grp_Transportation = expand_hierarchy_with_exclamation(sheets['Transportation'], sheets['Product Groups'], 'Product')
Grp_Transportation = expand_hierarchy_with_exclamation(Grp_Transportation, sheets['Period Groups'], 'Period')
Grp_Transportation = expand_hierarchy_with_exclamation(Grp_Transportation, sheets['Location Groups'], 'Origin')
Grp_Transportation = expand_hierarchy_with_exclamation(Grp_Transportation, sheets['Location Groups'], 'Destination')
Grp_Transportation = expand_hierarchy_with_exclamation(Grp_Transportation, sheets['Mode Groups'], 'Mode')
final_columns = [col for col in Grp_Transportation.columns if not col.startswith('Sub')]
Grp_Transportation = Grp_Transportation[final_columns]

Grp_QPU = expand_hierarchy_with_exclamation(sheets['QPU'], sheets['Product Groups'], 'Created')
Grp_QPU = expand_hierarchy_with_exclamation(Grp_QPU, sheets['Product Groups'], 'Product')
Grp_QPU = expand_hierarchy_with_exclamation(Grp_QPU, sheets['Period Groups'], 'Period')
Grp_QPU = expand_hierarchy_with_exclamation(Grp_QPU, sheets['Location Groups'], 'Location')
final_columns = [col for col in Grp_QPU.columns if not col.startswith('Sub')]
Grp_QPU = Grp_QPU[final_columns]

Sub_Production = expand_hierarchy(Grp_Production, sheets['Product Groups'], 'Product')
Sub_Production = expand_hierarchy(Sub_Production, sheets['Period Groups'], 'Period')
Sub_Production = expand_hierarchy(Sub_Production, sheets['Location Groups'], 'Location')
final_columns = [col for col in Sub_Production.columns if not col.startswith('Sub')]
Sub_Production = Sub_Production[final_columns]

Sub_Warehouses = expand_hierarchy(Grp_Warehouses, sheets['Product Groups'], 'Product')
Sub_Warehouses = expand_hierarchy(Sub_Warehouses, sheets['Period Groups'], 'Period')
Sub_Warehouses = expand_hierarchy(Sub_Warehouses, sheets['Location Groups'], 'Location')
final_columns = [col for col in Sub_Warehouses.columns if not col.startswith('Sub')]
Sub_Warehouses = Sub_Warehouses[final_columns]

Sub_Transportation = expand_hierarchy(Grp_Transportation, sheets['Product Groups'], 'Product')
Sub_Transportation = expand_hierarchy(Sub_Transportation, sheets['Period Groups'], 'Period')
Sub_Transportation = expand_hierarchy(Sub_Transportation, sheets['Location Groups'], 'Origin')
Sub_Transportation = expand_hierarchy(Sub_Transportation, sheets['Location Groups'], 'Destination')
Sub_Transportation = expand_hierarchy(Sub_Transportation, sheets['Mode Groups'], 'Mode')
final_columns = [col for col in Sub_Transportation.columns if not col.startswith('Sub')]
Sub_Transportation = Sub_Transportation[final_columns]

Sub_QPU = expand_hierarchy(Grp_QPU, sheets['Product Groups'], 'Created')
Sub_QPU = expand_hierarchy(Sub_QPU, sheets['Product Groups'], 'Product')
Sub_QPU = expand_hierarchy(Sub_QPU, sheets['Period Groups'], 'Period')
Sub_QPU = expand_hierarchy(Sub_QPU, sheets['Location Groups'], 'Location')
final_columns = [col for col in Sub_QPU.columns if not col.startswith('Sub')]
Sub_QPU = Sub_QPU[final_columns]


#Assigning ID Columns
Grp_Production['ID'] = Grp_Production['Location'].astype(str)+"_"+Grp_Production['Product'].astype(str)+"_"+Grp_Production['Period'].astype(str)
Sub_Production['ID'] = Sub_Production['Location'].astype(str)+"_"+Sub_Production['Product'].astype(str)+"_"+Sub_Production['Period'].astype(str)
Sub_Production['Original ID'] = Sub_Production['Original Location'].astype(str)+"_"+Sub_Production['Original Product'].astype(str)+"_"+Sub_Production['Original Period'].astype(str)
Grp_Warehouses['ID'] = Grp_Warehouses['Location'].astype(str)+"_"+Grp_Warehouses['Product'].astype(str)+"_"+Grp_Warehouses['Period'].astype(str)
Sub_Warehouses['ID'] = Sub_Warehouses['Location'].astype(str)+"_"+Sub_Warehouses['Product'].astype(str)+"_"+Sub_Warehouses['Period'].astype(str)
Sub_Warehouses['Original ID'] = Sub_Warehouses['Original Location'].astype(str)+"_"+Sub_Warehouses['Original Product'].astype(str)+"_"+Sub_Warehouses['Original Period'].astype(str)
Grp_Transportation['ID'] = Grp_Transportation['Mode'].astype(str)+"_"+Grp_Transportation['Product'].astype(str)+"_"+Grp_Transportation['Origin'].astype(str)+"_"+Grp_Transportation['Destination'].astype(str)+"_"+Grp_Transportation['Period'].astype(str)
Sub_Transportation['ID'] = Sub_Transportation['Mode'].astype(str)+"_"+Sub_Transportation['Product'].astype(str)+"_"+Sub_Transportation['Origin'].astype(str)+"_"+Sub_Transportation['Destination'].astype(str)+"_"+Sub_Transportation['Period'].astype(str)
Sub_Transportation['Original ID'] = Sub_Transportation['Original Mode'].astype(str)+"_"+Sub_Transportation['Original Product'].astype(str)+"_"+Sub_Transportation['Original Origin'].astype(str)+"_"+Sub_Transportation['Original Destination'].astype(str)+"_"+Sub_Transportation['Original Period'].astype(str)
Grp_QPU['ID'] = Grp_QPU['Location'].astype(str)+"_"+Grp_QPU['Created'].astype(str)+"_"+Grp_QPU['Product'].astype(str)+"_"+Grp_QPU['Period'].astype(str)
Sub_QPU['ID'] = Sub_QPU['Location'].astype(str)+"_"+Sub_QPU['Created'].astype(str)+"_"+Sub_QPU['Product'].astype(str)+"_"+Sub_QPU['Period'].astype(str)
Sub_QPU['Original ID'] = Sub_QPU['Original Location'].astype(str)+"_"+Sub_QPU['Original Created'].astype(str)+"_"+Sub_QPU['Original Product'].astype(str)+"_"+Sub_QPU['Original Period'].astype(str)
Sub_QPU['ID2'] = Sub_QPU['Location'].astype(str)+"_"+Sub_QPU['Product'].astype(str)+"_"+Sub_QPU['Period'].astype(str)
Demand = sheets['Demand'].copy()
Demand['ID'] = Demand['Customer'].astype(str)+"_"+Demand['Location'].astype(str)+"_"+Demand['Product'].astype(str)+"_"+Demand['Period'].astype(str)
Demand['ID2'] = Demand['Location'].astype(str)+"_"+Demand['Product'].astype(str)+"_"+Demand['Period'].astype(str)

#Dropping Duplicates from sub Production so list of IDs is unique and exhuastive
def combine_and_drop_duplicates(df, id_column, cost_columns):
    combined_df = df.groupby(id_column)[cost_columns].sum().reset_index()
    unique_df = df.drop_duplicates(subset=id_column).drop(columns=cost_columns)
    final_df = unique_df.merge(combined_df, on=id_column)
    final_df.reset_index(drop=True,inplace=True)
    return final_df

Sub_Production_W_Duplicates = Sub_Production.copy()
Sub_Warehouses_W_Duplicates = Sub_Warehouses.copy()
Sub_Transportation_W_Duplicates = Sub_Transportation.copy()
Sub_QPU_W_Duplicates = Sub_QPU.copy()

Sub_Production = combine_and_drop_duplicates(Sub_Production, id_column='ID', cost_columns=['Variable Cost', 'Lease Variable Cost', 'Other Variable Cost'])
Sub_Warehouses = combine_and_drop_duplicates(Sub_Warehouses, id_column='ID', cost_columns=['Variable Cost', 'Lease Variable Cost', 'Other Variable Cost', 'Beginning Inventory'])
Sub_Transportation = combine_and_drop_duplicates(Sub_Transportation, id_column='ID', cost_columns=['Variable Cost', 'Lease Variable Cost', 'Other Variable Cost', 'OBC Volume'])
Sub_QPU = Sub_QPU[Sub_QPU['Product'] != Sub_QPU['Created']]
Sub_QPU = combine_and_drop_duplicates(Sub_QPU, id_column='ID', cost_columns=[])

#Initializing Gurobi with my key
keys = {"GURO_PAR_ISVNAME": "Infor",
"GURO_PAR_ISVAPPNAME": "Infor" ,
"GURO_PAR_ISVEXPIRATION": 20270930,
"GURO_PAR_ISVKEY": "G9FKJ8IG"}

with gp.Env(params=keys) as env:
    with gp.Model(env=env) as model:
#   Initialize Variables
        Production = model.addVars(Sub_Production['ID'].tolist(),vtype='C', name ='Production')
        Beg_Inv = model.addVars(Sub_Warehouses['ID'].tolist(), vtype='C', name = 'Beg_Inv')
        QpU = model.addVars(Sub_QPU['ID'].tolist(), vtype='C', name = 'QpU')
        End_Inv = model.addVars(Sub_Warehouses['ID'].tolist(), vtype='C', name = 'End_Inv')
        Lane = model.addVars(Sub_Transportation['ID'].tolist(), vtype='C', name = 'Lane')
        if Historicals == 0:
            APosition = model.addVars(Grp_Warehouses['ID'].tolist(), vtype='C', name = 'APosition', lb = -500000000, ub = 500000000)                                   
            Abs_Position = model.addVars(Grp_Warehouses['ID'].tolist(), vtype='C', name = 'Abs_Position', lb = 0, ub = 500000000)  
            Position_Expense = model.addVars(Grp_Warehouses['ID'].tolist(), vtype='C', name = 'Position_Expense')

# Link Sum of QPU created to Production
        for i in range(0,len(Sub_Production)):
            Location = Sub_Production['Location'].tolist()[i]
            Product = Sub_Production['Product'].tolist()[i]
            Period = Sub_Production['Period'].tolist()[i]
            FLst_QPU = Sub_QPU[(Sub_QPU['Location']==Location) & 
                    (Sub_QPU['Created']==Product) & 
                    (Sub_QPU['Period']==Period)]['ID'].tolist()
            if len(FLst_QPU)>0:
                model.addConstr(Production[Sub_Production['ID'][i]] == sum(QpU[j] for j in FLst_QPU))

#   Calculating ending inventory off of starting inventory production and transportation movements
        for i in range(0,len(Sub_Warehouses)):
            Wh_ID = Sub_Warehouses['ID'].tolist()[i]
            Location = Sub_Warehouses['Location'].tolist()[i]
            Product = Sub_Warehouses['Product'].tolist()[i]
            Curr_Period = Sub_Warehouses['Period'].tolist()[i]
            FLst_In_Lanes = Sub_Transportation[(Sub_Transportation['Destination']==Location) & 
                                               (Sub_Transportation['Product']==Product) & 
                                               (Sub_Transportation['Period']==Curr_Period)]['ID'].tolist()

            FLst_Out_Lanes = Sub_Transportation[(Sub_Transportation['Origin']==Location) & 
                                                (Sub_Transportation['Product']==Product) & 
                                                (Sub_Transportation['Period']==Curr_Period)]['ID'].tolist()
            FLst_Production = Sub_Production[(Sub_Production['Location']==Location) & 
                                             (Sub_Production['Product']==Product) & 
                                             (Sub_Production['Period']==Curr_Period)]['ID'].tolist()
            FLst_QPU = Sub_QPU[(Sub_QPU['Location']==Location) & 
                               (Sub_QPU['Product']==Product) & 
                               (Sub_QPU['Period']==Curr_Period) & 
                               (Sub_QPU['ID2'].isin(Sub_Production['ID'].tolist()))]['ID'].tolist()
            FLst_QPU2 = Sub_QPU[(Sub_QPU['Location']==Location) & 
                                (Sub_QPU['Product']==Product) & 
                                (Sub_QPU['Period']==Curr_Period) & 
                                (Sub_QPU['ID2'].isin(Sub_Production['ID'].tolist()))]['QPU'].tolist()
            FLst_Demand = Demand[Demand['ID2'] == Wh_ID]['Demand'].tolist()
            Linked_Production = sum(QpU[FLst_QPU[j]]*FLst_QPU2[j]for j in range(0,len(FLst_QPU)))

            model.addConstr(Beg_Inv[Wh_ID] + sum(Production[j] for j in FLst_Production) + sum(Lane[j] for j in FLst_In_Lanes) - sum(Lane[j] for j in FLst_Out_Lanes) 
                            - sum(QpU[FLst_QPU[j]]*FLst_QPU2[j]for j in range(0,len(FLst_QPU)))- sum(j for j in FLst_Demand) == End_Inv[Wh_ID])
#   Setting Beginning Inventory for each period: if initial period it is set through the input dataset if it is a subsequent period it is linked to the previous periods ending inventory
            if Curr_Period == initial_Period:
                model.addConstr(Beg_Inv[Wh_ID] == Sub_Warehouses['Beginning Inventory'].tolist()[i])
            else:
                Prev_Period = sdf_Periods[sdf_Periods['Order'] == sdf_Periods[sdf_Periods['Period']==Sub_Warehouses['Period'][i]]
                                          ['Order'].tolist()[0]-1]['Period'].tolist()[0]
                Prev_Wh_ID = Sub_Warehouses[(Sub_Warehouses['Location'] ==Location) &
                                            (Sub_Warehouses['Product'] == Product) &
                                            (Sub_Warehouses['Period'] == Prev_Period)]['ID'].tolist()[0]
                model.addConstr(Beg_Inv[Wh_ID] == End_Inv[Prev_Wh_ID])
#   Demand is fully sourced, if this is a historical model a error band is given to remove input rounding errors
        for i in range(0,len(Demand)):
            Dem_ID = Demand['ID'][i]
            Customer = Demand['Customer'][i]
            Location = Demand['Location'][i]
            Product = Demand['Product'][i]
            Period = Demand['Period'][i]
            FLst_Lanes = Sub_Transportation[(Sub_Transportation['Destination']==Location) & 
                                            (Sub_Transportation['Product']==Product) &
                                            (Sub_Transportation['Period']==Period)]['ID'].tolist()
        
            FLst_Demand = Demand[Demand['ID2'] == Demand['ID2'][i]]['Demand'].tolist()
            if Historicals == 1:
                Err_Band = sheets['Settings'][sheets['Settings']['Setting'] == 'Demand Error Band']['Value'].tolist()[0]
                model.addConstr(sum(Lane[j] for j in FLst_Lanes) <= sum(j for j in FLst_Demand)*(1+Err_Band))
                model.addConstr(sum(Lane[j] for j in FLst_Lanes) >= sum(j for j in FLst_Demand)*(1-Err_Band))
            else:
                model.addConstr(sum(Lane[j] for j in FLst_Lanes) == sum(j for j in FLst_Demand))

#   Production Capacities
        for i in range(0,len(Grp_Production)):
            Lst_ID = Sub_Production_W_Duplicates[Sub_Production_W_Duplicates['Original ID'] == Grp_Production['ID'][i]]['ID'].tolist()
            if Historicals == 1:
                if Grp_Production['OBC Volume'][i]>0:
                    model.addConstr( sum(Production[n] for n in Lst_ID) == Grp_Production['OBC Volume'][i])
            else:
                if Grp_Production['Maximum Production'][i]>0:
                    model.addConstr( sum(Production[n] for n in Lst_ID) <= Grp_Production['Maximum Production'][i])
                if Grp_Production['Minimum Production'][i]>0:
                    model.addConstr(sum(Production[n] for n in Lst_ID) >= Grp_Production['Minimum Production'][i])
#   Warehouse Capacities only get set during forecasted model to avoid over constraining historic model
        if Historicals == 0:
            for i in range(0,len(Grp_Warehouses)):
                Lst_ID = Sub_Warehouses_W_Duplicates[Sub_Warehouses_W_Duplicates['Original ID'] == Grp_Warehouses['ID'][i]]['ID'].tolist()
                if Grp_Warehouses['Maximum Capacity'][i]>0:
                    model.addConstr( sum(End_Inv[n] for n in Lst_ID) <= Grp_Warehouses['Maximum Capacity'][i])
                if Grp_Warehouses['Minimum Capacity'][i]>0:
                    model.addConstr(Grp_Warehouses['Minimum Capacity'][i] <= sum(End_Inv[n] for n in Lst_ID))
                if Grp_Warehouses['Target Inventory'][i] > 1:
                    Wh_ID = Grp_Warehouses['ID'][i]
                    model.addConstr(APosition[Wh_ID] == sum(End_Inv[n] for n in Lst_ID) - Grp_Warehouses['Target Inventory'][i])
                    model.addConstr(Abs_Position[Wh_ID] == gp.abs_(APosition[Wh_ID]))
                    model.addConstr(Position_Expense[Wh_ID]==((Abs_Position[Wh_ID])/1000*(Abs_Position[Wh_ID])/1000)*Grp_Warehouses['Violation Cost'][i])
#   Transportation Capacities
        for i in range(0,len(Grp_Transportation)):
            Lst_ID = Sub_Transportation_W_Duplicates[Sub_Transportation_W_Duplicates['Original ID'] == Grp_Transportation['ID'][i]]['ID'].tolist()
            if Historicals == 1:
                if Grp_Transportation['OBC Volume'][i]>0:
                    model.addConstr( sum(Lane[n] for n in Lst_ID) == Grp_Transportation['OBC Volume'][i])
            else:
                if Grp_Transportation['Maximum Capacity'][i]>0:
                    model.addConstr( sum(Lane[n] for n in Lst_ID) <= Grp_Transportation['Maximum Capacity'][i])
                if Grp_Transportation['Minimum Capacity'][i]>0:
                    model.addConstr(sum(Lane[n] for n in Lst_ID) >= Grp_Transportation['Minimum Capacity'][i])

#setting up objective function
        obj_function=0

        Sub_Production['Total Variable Cost'] = (Sub_Production['Variable Cost'] + Sub_Production['Lease Variable Cost'] + Sub_Production['Other Variable Cost'])
        Sub_Production['Cost'] = Sub_Production['ID'].map(Production) * Sub_Production['Total Variable Cost']
        obj_function -= Sub_Production['Cost'].sum()

        Grp_Production['Total Fixed Cost'] = Grp_Production['Fixed Cost'] + Grp_Production['Lease Fixed Cost'] + Grp_Production['Other Fixed Cost']
        obj_function -= Grp_Production['Total Fixed Cost'].sum()

        Sub_Transportation['Total Variable Cost'] = (Sub_Transportation['Variable Cost'] + Sub_Transportation['Lease Variable Cost'] + Sub_Transportation['Other Variable Cost'])
        Sub_Transportation['Cost'] = Sub_Transportation['ID'].map(Lane) * Sub_Transportation['Total Variable Cost']
        obj_function -= Sub_Transportation['Cost'].sum()

        Grp_Transportation['Total Fixed Cost'] = Grp_Transportation['Fixed Cost'] + Grp_Transportation['Lease Fixed Cost'] + Grp_Transportation['Other Fixed Cost']
        obj_function -= Grp_Transportation['Total Fixed Cost'].sum()
# Warehouse costs are applied differently due to the way our logistics group thinks about costs. they see the costs being applied to any product that touches the warehouse during a given period. 
        #Inheriently if product touches a warehouse it either stays there or leaves so the sum of all lanes out and the ending inventory equals the volume a warehouse saw.
        Sub_Warehouses['Total Variable Cost'] = (Sub_Warehouses['Variable Cost'] + Sub_Warehouses['Lease Variable Cost'] + Sub_Warehouses['Other Variable Cost'])
        for i in range(0,len(Sub_Warehouses)):
            Lane_ID = Sub_Transportation[(Sub_Transportation['Origin'] == Sub_Warehouses['Location'][i]) & 
                                                                           (Sub_Transportation['Product'] ==  Sub_Warehouses['Product'][i]) & 
                                                                           (Sub_Transportation['Period'] ==  Sub_Warehouses['Period'][i])]['ID'].tolist()
            Tot_Wh_Vol = End_Inv[Sub_Warehouses['ID'][i]] + sum(Lane[j] for j in Lane_ID)
            obj_function -= (End_Inv[Sub_Warehouses['ID'][i]] + sum(Lane[j] for j in Lane_ID))*Sub_Warehouses['Total Variable Cost'][i]
        
        Grp_Warehouses['Total Fixed Cost'] = Grp_Warehouses['Fixed Cost'] + Grp_Warehouses['Lease Fixed Cost'] + Grp_Warehouses['Other Fixed Cost']
        obj_function -= Grp_Warehouses['Total Fixed Cost'].sum()
#Position expense only applies if model is Forecasted
        if Historicals == 0:
            Grp_Warehouses['Position Expense'] = Grp_Warehouses.apply(
                lambda row: Position_Expense[row['ID']] if row['Target Inventory'] > 1 else 0, axis=1)
            obj_function -= Grp_Warehouses['Position Expense'].sum()
        obj_function += (Demand['Demand'] * Demand['Variable Revenue']).sum()
#setting objective function into model and maximize the funciton then running optimization
        model.setObjective(obj_function, gp.GRB.MAXIMIZE)
        model.optimize()
#In development : this is for a future feature to help determine why a model is infeasible. This runs the model again but allows the model to violate boundries
        #first argument in feasRelaxS: 0 = minimize sum of violation, 1 = minimize square of violations, 2 = minimize number of violations
        # if model.Status == 3:
        #     model.feasRelaxS(0,False,True,True)
        #     model.optimize()
#writing the results back to the Sub Dataframes
        Sub_Production['Results'] = Sub_Production['ID'].apply(lambda i: round(Production[i].X,ndigits=2))
        Sub_Warehouses['Beginning Inventory'] = Sub_Warehouses['ID'].apply(lambda i: round(Beg_Inv[i].X,ndigits=2))
        Sub_Warehouses['Ending Inventory'] = Sub_Warehouses['ID'].apply(lambda i: round(End_Inv[i].X,ndigits=2))
        Sub_Transportation['Results'] = Sub_Transportation['ID'].apply(lambda i: round(Lane[i].X,ndigits=2))
        Sub_Warehouses['Results'] = 0.0
        for i in range(0,len(Sub_Warehouses)):
            FLst_Out_Lanes = Sub_Transportation[(Sub_Transportation['Origin'] == Sub_Warehouses['Location'][i]) & 
                                        (Sub_Transportation['Product'] ==  Sub_Warehouses['Product'][i]) & 
                                        (Sub_Transportation['Period'] ==  Sub_Warehouses['Period'][i])]['Results'].tolist()
            Tot_Wh_Vol = Sub_Warehouses['Ending Inventory'][i] + sum(FLst_Out_Lanes)
            Sub_Warehouses.at[i, 'Results'] = Tot_Wh_Vol
        Sub_QPU['Results'] = Sub_QPU['ID'].apply(lambda i: round(QpU[i].X,ndigits=2))
        Grp_Production['Results'] = Grp_Production['ID'].apply(lambda i: round(Sub_Production[Sub_Production['Original ID'] == i]['Results'].sum(),ndigits=2))
        Grp_Warehouses['Beginning Inventory'] = Grp_Warehouses['ID'].apply(lambda i: round(Sub_Warehouses[Sub_Warehouses['Original ID'] == i]['Beginning Inventory'].sum(),ndigits=2))
        Grp_Warehouses['Ending Inventory'] = Grp_Warehouses['ID'].apply(lambda i: round(Sub_Warehouses[Sub_Warehouses['Original ID'] == i]['Ending Inventory'].sum(),ndigits=2))
        Grp_Transportation['Results'] = Grp_Transportation['ID'].apply(lambda i: round(Sub_Transportation[Sub_Transportation['Original ID'] == i]['Results'].sum(),ndigits=2))
        Grp_QPU['Results'] = Grp_QPU['ID'].apply(lambda i: round(Sub_QPU[Sub_QPU['Original ID'] == i]['Results'].sum(),ndigits=2))

#Printing Results to Output File: filtering out any unused locations to clean up results and not overload the viewer
fdf_Production = Sub_Production[Sub_Production['Results']>0]
fdf_Warehouses = Sub_Warehouses[Sub_Warehouses['Results'] >0]
fdf_Transportation = Sub_Transportation[Sub_Transportation['Results']>0]
fdf_QPU = Sub_QPU[Sub_QPU['Results']>0]

with pd.ExcelWriter(Output_File, mode="a", engine="openpyxl",if_sheet_exists='replace') as writer:
    fdf_Production.to_excel(writer, sheet_name='Sub_Production')
    fdf_Warehouses.to_excel(writer, sheet_name='Sub_Warehouses')
    fdf_Transportation.to_excel(writer, sheet_name='Sub_Transportation')
    fdf_QPU.to_excel(writer,sheet_name='Sub_QPU')
    Grp_Production.to_excel(writer, sheet_name='Grp_Production')
    Grp_Warehouses.to_excel(writer, sheet_name='Grp_Warehouses')
    Grp_Transportation.to_excel(writer, sheet_name='Grp_Transportation')
    Grp_QPU.to_excel(writer, sheet_name='Grp_QPU')

run_time = time.time()-start
print("Run Time =",f"{round(run_time,ndigits=3):,}")
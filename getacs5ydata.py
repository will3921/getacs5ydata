import requests
import json
import pandas as pd
import math

census_key = "61212d92f407e4d6967d8843dc3a2d6592d8c3b5"


def todfvar(dflist, numvariables):
   initial_df = pd.DataFrame(data=dflist[0], columns=[dflist[0][0][0], "state",
                                        "county", "tract"])
   variable_list = list(range(numvariables + 1))
   variable_list[numvariables] = "fips_code"
   initial_df["fips_code"] = 0
   for x in range(len(initial_df)):
       state, county, tract = initial_df.iloc[x,1], initial_df.iloc[x,2], initial_df.iloc[x,3]
       fips = str(state) + str(county) + str(tract)
       initial_df.iloc[x,4] = fips
   initial_df = initial_df.loc[:, "fips_code"]
   initial_df = initial_df.to_frame()
   
   for x in range(numvariables):
       current_list = dflist[x]
       current_df = pd.DataFrame(data=current_list, columns=[current_list[0][0], "state",
                                        "county", "tract"])
       
       current_df["fips_code1"] = 0
       
       variable_list[x] = current_list[0][0]
       
       for y in range(len(current_df)):
           state, county, tract = current_df.iloc[y,1], current_df.iloc[y,2], current_df.iloc[y,3]
           fips = str(state) + str(county) + str(tract)
           current_df.iloc[y,4] = fips
           
       
       initial_df = initial_df.merge(current_df, how="left", left_on="fips_code", right_on="fips_code1") 
   initial_df = initial_df.loc[:,variable_list]         
   initial_df = initial_df.drop(initial_df.index[0])
       
   return initial_df

def getacs5variables(variables, state):
    """
    

    Parameters
    ----------
    variables : list
        list of variables intended
    state : int
        census state code, put * for all data

    Raises
    ------
    Exception
        Invalid Variable

    Returns
    -------
    stdf : DataFrame
        data
    filtered_md : dict
        meta data

    """
    with open("acs5stvariables.json") as json_file:
        metadata = json.load(json_file)
    metadata = metadata["variables"]
    
    try:
        for variable in variables:
            assert variable in metadata.keys()
            
    except:
        raise Exception("Invalid Variable")

    
    data = list(range(len(variables)))
    for x in range(len(variables)):
        variable = variables[x]
        
        baseurl1 = "https://api.census.gov/data/2019/acs/acs5/subject?get="
        baseurl2 = "&for=tract:*&in=state:"
        baseurl3 = "&key="
            
        url = baseurl1 + str(variable) + baseurl2 + str(state) + baseurl3 + census_key
            
        response = requests.get(url)
        apiquerry = response.json() 
        data[x] = apiquerry
        
        
    df = todfvar(data, len(data))
    
    
    filtered_md = {}
    for x in range(len(variables)):
        variable = variables[x]
        filtered_md[variable] = metadata[variable]
    
    return df, filtered_md
    

def getacs5subjecttables(subjecttable, state):
    """
    

    Parameters
    ----------
    subjecttable : string
        census code for intended subject table
        
    state : int
        census state code, put * for all data

    Returns
    -------
    stdf : DataFrame
        data
    filtered_md : dict
        metadata

    """
    with open("acs5stvariables.json") as json_file:
        metadata = json.load(json_file)
    metadata = metadata["variables"]
    metadf = pd.DataFrame.from_dict(metadata, orient="index")
    metadf = metadf.reset_index()
    metadf = metadf[(metadf.group == subjecttable)]
    variables = list(metadf.iloc[:,0])
    
    num_querries = math.ceil(len(variables) / 50)
    
    bottom = 0
    top = 49
        
    baseurl1 = "https://api.census.gov/data/2019/acs/acs5/subject?get="
    baseurl2 = "&for=tract:*&in=state:"
    baseurl3 = "&key="
    
    data = list(range(num_querries))
    
    for x in range(num_querries - 1):
        variables_str = ",".join(variables[bottom:top])
    
        url = baseurl1 + variables_str + baseurl2 + str(state) + baseurl3 + census_key
    
        response = requests.get(url)
        apiquerry = response.json() 
        data[x] = apiquerry
        
        bottom = bottom + 50
        top = top + 50
    
    top = len(variables)
    variables_str = ",".join(variables[bottom:top])
    url = baseurl1 + variables_str + baseurl2 + str(state) + baseurl3 + census_key
    response = requests.get(url)
    apiquerry = response.json() 
    data[-1] = apiquerry
    
    fips_list = list(range(len(data[0])))
    for x in range(len(data[0])):
        fips_list[x] = str(data[0][x][len(data[0][0])-3] + data[0][x][len(data[0][0])-2] + data[0][x][len(data[0][0])-1])
    stdf = pd.DataFrame(data=fips_list, columns=["fips_code_good"])
    
    for x in range(len(data)):
        current_data = data[x]
        current_data = pd.DataFrame(data=current_data, columns=current_data[0])
        current_data["fips_code"] = current_data["state"] + current_data["county"] + current_data["tract"]
        stdf = stdf.merge(current_data, how="left", left_on="fips_code_good", right_on="fips_code") 
    
    filtered_md = {}
    for x in range(len(variables)):
        variable = variables[x]
        filtered_md[variable] = metadata[variable]
    
    
    #variables.append("fips_code_good")
    #stdf = stdf[:,variables] 
    #return variables
    
    try:
        
        stdf = stdf.drop(["state", "county", "tract", "fips_code"], axis=1)
        error = "None"
        
    except:
        error = "None"
        
    try:
        
        stdf = stdf.drop(["state_x", "county_x", "tract_x", "fips_code_x"], axis=1)
        error = "None"
        
    except:
        error = "None"
        
    try:
        
        stdf = stdf.drop(["state_y", "county_y", "tract_y", "fips_code_y"], axis=1)
        error = "None"
        
    except:
        error = "None"
        
    assert error == "None"
        
    stdf = stdf.rename(columns={"fips_code_good": "fips_code"})
        
    return stdf, filtered_md
    

getacs5subjecttables()



#! /user/nkerman/miniconda3/envs/cosmo_env/bin/python
# %%
# Imports cell:
import enum
from os import read
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from astropy.time import Time
import subprocess
import pytimedinput
import numpy as np
# %%
<<<<<<< HEAD
# USER INPUTS (those not queried on CLI):
selected_filetypes = ['LD2LMP1T']#,'LMMCETMP'] # TODO: check/remove this limit
TIMEOUT=0.5
=======
# USER INPUTS:
selected_filetypes = ['LD2LMP1T','LMMCETMP']
TIMEOUT=0.0
>>>>>>> 4b2f37246268e098c03e02045aff4bc5a845ea84
telemetry_dir = Path("/grp/hst/cos/Telemetry/")
plots_dir = Path("/user/nkerman/Projects/Monitors/telemetry_plots/")
# %%
# Find all the telemetry data files:
all_files = list(telemetry_dir.glob('*'))
file_dict = {}
for file in telemetry_dir.glob('*'):
    if (not file.is_dir()) and (file.is_file()):
        ftype = file.name
        file_dict[ftype] = file

# %%
def read_data(filetype, verbose=False):
    # Read in the data to a pandas dataframe:
    read_data_full = pd.read_csv(
    filetype,
    delim_whitespace=True,
    header=None,
    names=['MJD','Data']
    )

    # Convert Modified Julian date into datetime object:
    datetime_arr = Time(read_data_full['MJD'],format='mjd').to_datetime() 
    # Add datetime info as new column in dataframe:
    read_data_full['datetime'] = datetime_arr
    if verbose:
        print(f"Reading in {filetype}")
    return read_data_full

# %%
def my_input(prompt, timeout=10.):
    user_string, timedout_bool = pytimedinput.timedInput(prompt,timeout)
    print(timedout_bool)
    if timedout_bool:
        user_string = ""
    return user_string
# %%
# This cell checks for user input, 
# However it only gives until TIMEOUT seconds to respond, then sets defaults.
def ask_user_dates(verbose=False, timeout=10.):
    try: # In case non-interactive terminal, set to default:
        user_min_date = my_input(f"Start date? [{timeout} seconds to respond],[Enter for default {def_min_date}]", TIMEOUT)
    except:
        user_min_date = def_min_date
        print(f"setting to default of {def_min_date}")
    if user_min_date in ["N","n",None]:
        user_min_date = def_min_date
    try: 
        float(user_min_date)
    except ValueError:
        if verbose: 
            print(f"could not convert string to float; setting to default of {def_min_date}")
        user_min_date = def_min_date
    mindex, min_date = find_closest_date(read_data_full,float(user_min_date))
    try: # In case non-interactive terminal, set to default:
        user_max_date = def_max_date
        user_max_date = my_input(f"End date?   [{timeout} seconds to respond],[Enter for default {def_max_date}]", TIMEOUT)
    except:
        print(f"setting to default of {def_min_date}")
    if user_max_date in ["N","n",None]:
        user_max_date = def_max_date
    try: 
        float(user_max_date)
    except ValueError:
        if verbose: 
            print(f"could not convert string to float; setting to default of {def_max_date}")
        user_max_date = def_max_date

    maxdex, max_date = find_closest_date(read_data_full,float(user_max_date))
    if verbose: 
        print(min_date,max_date)

    return mindex, user_min_date, maxdex, user_max_date
# %%
def get_quantiles(dataframe, q_low=0.005, q_hi=0.995):
    """
    Defaults to containing central 99% of the data
    """
    miny, maxy = dataframe['Data'].quantile(q_low),dataframe['Data'].quantile(q_hi)
    return miny, maxy
# %%
def find_closest_date(dataframe, target, verbose=False, category='MJD'):
    found_row = dataframe.iloc[(dataframe[category] - target).abs().argsort()[0]]
    if verbose: 
        print(found_row)
    return found_row.name,found_row[category]
# %%
def build_plot(dataframe, filetype, plot_by="datetime", plot_quantbox=True, q_low=0.005, q_hi=0.995, open_file=False, show_plot=False):
    fig = go.Figure()

    miny,maxy = get_quantiles(dataframe, q_low, q_hi)
    # Date limits as MJD:
    minx, maxx = trimmed_data['MJD'].min(),trimmed_data['MJD'].max()
    # Or Date limits as datetimes
    minx_dt,maxx_dt = Time(minx,format='mjd').to_datetime(), Time(maxx,format='mjd').to_datetime() 

    if plot_quantbox:
        if plot_by == "mjd":
            x_box=[minx,maxx,maxx,minx,minx]
            y_box=[miny,miny,maxy,maxy,miny]
        elif plot_by == "datetime":
            x_box=[minx_dt,maxx_dt,maxx_dt,minx_dt,minx_dt]
            y_box=[miny,miny,maxy,maxy,miny]
        else:
            print("Not a valid plot_by selection ['mjd', 'datetime']")
            exit

        fig.add_trace(go.Scattergl(
            x=x_box, 
            y=y_box,
            text=f"Quantile range: {q_low} - {q_hi}",
            name=f'99% Range',
            fill="toself",
            fillcolor='rgba(256,220,150,0.2)',
            line=None))
    
    fig.add_trace(
        go.Scattergl(x=dataframe['datetime'],y=dataframe['Data'], mode="markers", name=f'{filetype}')
        )

    # fig.show()
    new_filename = f'{plots_dir}/{filetype}_{minx:.1f}to{maxx:.1f}.html'
    if show_plot:
        fig.show()
    fig.write_html(new_filename)


    if open_file:
        subprocess.run(['open', new_filename], check=True)
# %%

for item_num, filetype in enumerate(file_dict.keys()):

    # while item_num < 2: # If you want to limit to the first N files
    
    # if filetype in selected_filetypes: # or if want to limit to a set of filetypes
        
        try:
            print(f"{item_num}: Running for {filetype}")
            read_data_full = read_data(telemetry_dir/filetype)

            # Set the default min/max date of the plot to the last year since most recent point:
            def_max_date = read_data_full['MJD'].iloc[-1]
            def_min_date = def_max_date - 365.25

            # Solicit the min/max date of the plot from the user, with defaults if no numerical date detected
            mindex, user_min_date, maxdex, user_max_date = ask_user_dates(verbose = False, timeout=TIMEOUT)

            # Actually cut the data to that size:
            trimmed_data = read_data_full[mindex:maxdex]

            build_plot(dataframe=trimmed_data, filetype=filetype ,plot_by="datetime", plot_quantbox=True, q_low=0.005, q_hi=0.995, open_file=False, show_plot=False)
        except Exception as ex:
            print(f"Something went wrong on file: {filetype}")
# %% # NEW STUFF HERE!
def step_wise(model_dataframe, targ_x, category="MJD", step_pos="right"):
    """
    A stepwise function that rises/falls at the location of the next datapoint
      i.e. if: x=[1,3,9] and y=[0,1,0]
         then: the function rises at 3, and falls at 9
    
    Pseudocode:
    * find the nearest (on the left/lower end) value in xarr to the input targ_x
    * find the index of that xarr value
    * return the yarr value at that index
    """
    found_row = model_dataframe.iloc[(model_dataframe[category] - targ_x).abs().argsort()[0]]
    found_selector = found_row[category]
    closest_index=found_row.name
    if step_pos == "right":
        if targ_x < found_selector:
            closest_index -= 1
    return model_dataframe.iloc[closest_index]

# %%
def step_wise2(model_dataframe, target):
    """
    Equivalent to step_wise and not much faster (tested it).
    """
    delta = model_dataframe['MJD'] - target
    abs_delta = abs(delta)
    closest_index = abs_delta.argsort()[0]
    if delta[closest_index] > 0:
        closest_index = closest_index - 1
    return model_dataframe['Data'][closest_index]


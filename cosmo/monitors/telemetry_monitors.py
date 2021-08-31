#!/usr/bin/env python
# %%
# Initial quick-loading imports:
import argparse
from os import environ
from sys import exit as sysex
from pathlib import Path
# USER INPUTS (those not queried on CLI):
selected_filetypes = ['LMMCETMP','LOSMLAMB','LOSM1POS','LOSM2POS','LD2LMP1T'] # Only filters to these if the if statement below is not commented out (search for "selected_filetypes")
TIMEOUT=15.0 # TODO: frequently check/remove this limit
color_by_data_list = ['LOSMLAMB','LOSM1POS','LOSM2POS'] # Do you want to color the datapoints based on their y value?
skip_quantbox_list = ['LOSMLAMB','LOSM1POS','LOSM2POS'] # Do you want to skip plotting the default quantile box (encloses 99% of datapoints by default)
class bcolors: # We may wish to print some colored output to the terminal
    # citation https://stackoverflow.com/questions/287871/how-to-print-colored-text-to-the-terminal
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    DKGREEN = '\033[32m'
    OKGREEN = '\033[92m'
    DKRED = '\033[31m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Deal with CLI Arguments:

parser = argparse.ArgumentParser()
parser.add_argument("-v", "--verbose", help="increase output verbosity (Boolean)", action="store_true")
parser.add_argument("-u", "--ask-user", help="Ask user for each Telemetry file's begin/end dates (Boolean). Note, these are overridden by any begindate/enddate specified!", action="store_true")
parser.add_argument("-t", "--timeout", type=float, help=f"If asking the user for begin/end dates, how long to give them to respond before setting default. Defaults to {bcolors.DKRED}{TIMEOUT}{bcolors.ENDC}")
parser.add_argument("-b", "--begindate", type=float, help="The date you'd like to begin the monitors on. If none specified, defaults to 365.25 days before last datapoint in Telemetry file or --ask-user. If specified, overrides ask-user dates.")
parser.add_argument("-e", "--enddate", type=float, help="The date you'd like to begin the monitors on. If none specified, defaults day of last datapoint in Telemetry file or --ask-user. If specified, overrides ask-user dates.")
parser.add_argument("-o", "--outdir", type=str, help=f"The directory to save plots in. If specified, overrides the directory specified by the environment variable '{bcolors.DKGREEN}TELEMETRY_PLOTSDIR{bcolors.ENDC}'.")
parser.add_argument("-f", "--filetypes", type=str, help=f"The filetypes to run the monitors code on. Must be in the {bcolors.DKGREEN}TELEMETRY_DATADIR{bcolors.ENDC} location", nargs='+')
args = parser.parse_args()
if args.verbose:
    print("Verbosity turned on...")
    verbose = True
else:
    verbose = False

try:
    if args.outdir:
        plots_dir = Path(args.outdir)
    else:
        plots_dir = Path(environ['TELEMETRY_PLOTSDIR'])
    osm_plots_dir = plots_dir/"OSM_plots/"
    telemetry_dir = Path(environ['TELEMETRY_DATADIR'])
    mnemonics_file = Path(environ['TELEMETRY_MNEMONICS'])
    zooms_file = Path(environ['TELEMETRY_ZOOMS'])
    texts_file = Path(environ['TELEMETRY_TEXTS'])
    plots_dir.mkdir(exist_ok=True)
    osm_plots_dir.mkdir(exist_ok=True)
except Exception as nameExcept:
    print(nameExcept)
    print(f"""
    It seems that you are lacking some of the necessary environment variables. These include:
    {bcolors.DKGREEN}TELEMETRY_DATADIR{bcolors.ENDC} : Path to {bcolors.OKCYAN}directory{bcolors.ENDC} containing all the telemetry files 
    {bcolors.DKGREEN}TELEMETRY_ZOOMS{bcolors.ENDC} : Path to the {bcolors.OKCYAN}(CSV) file{bcolors.ENDC} containing your list of default zooms for each telemetry variable
    {bcolors.DKGREEN}TELEMETRY_MNEMONICS{bcolors.ENDC} : Path to the {bcolors.OKCYAN}(Excel) file{bcolors.ENDC} containing your list of mnemonics for each telemetry variable
    {bcolors.DKGREEN}TELEMETRY_TEXTS{bcolors.ENDC} : Path to the {bcolors.OKCYAN}(JSON) file{bcolors.ENDC} where you specify the numerical value of each telemetry variable whose value is a text string (i.e. the OSM positions)
    {bcolors.DKGREEN}TELEMETRY_PLOTSDIR{bcolors.ENDC} : Path to {bcolors.OKCYAN}directory{bcolors.ENDC} where you want to save the output plots of each telemetry variable

    We recommend checking and sourcing your {bcolors.OKBLUE}'cosmo/cosmo/telemetry_support/telem_mon_env_variables.sh'{bcolors.ENDC} file
    """)
    sysex(1)
# %%
# Slower imports:
import pandas as pd
import plotly.graph_objects as go
from astropy.time import Time
import subprocess
if args.ask_user: # If querying user with timeout import a timed input pkg
    import pytimedinput
    if args.timeout:
        TIMEOUT = args.timeout
# %%
# Read in the file which tells us what the filenames mean:
mnemon_df = pd.read_excel(mnemonics_file, sheet_name=0)
# Read in the file which specifies manual zooms/limits:
zoom_df = pd.read_csv(zooms_file)
# %%
# Find all the telemetry data files:
all_files = list(telemetry_dir.glob('*'))
file_dict = {}
for file in telemetry_dir.glob('*'):
    if (not file.is_dir()) and (file.is_file()):
        ftype = file.name
        file_dict[ftype] = file

if args.filetypes:
    selected_filetypes = [filetype.upper() for filetype in args.filetypes if filetype.upper() in list(file_dict.keys())]
    assert len(selected_filetypes) > 0, f"If specifying filetypes, you must actually list some filetypes in the{bcolors.DKGREEN} TELEMETRY_DATADIR{bcolors.ENDC} directory."
else:
    selected_filetypes = list(file_dict.keys())
if verbose:
    print(f"We will create monitors for these {len(selected_filetypes)} files:\n{selected_filetypes}")
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
    if timedout_bool:
        user_string = ""
    return user_string
# %%
# This cell checks for user input, 
# However it only gives until TIMEOUT seconds to respond, then sets defaults.
def ask_user_dates(verbose=False, timeout=10.):
    try: # In case non-interactive terminal, set to default:
        user_min_date = my_input(f"Start date? [{timeout} seconds to respond],[Enter for default {def_min_date}]", timeout)
    except:
        user_min_date = def_min_date
        print(f"setting to default of {def_min_date} unless overridden by begindate/enddate arguments.")
    if user_min_date in ["N","n",None]:
        user_min_date = def_min_date
    try: 
        float(user_min_date)
    except ValueError:
        if verbose: 
            print(f"could not convert string to float; setting to default of {def_min_date} unless overridden by begindate/enddate arguments.")
        user_min_date = def_min_date
    mindex, min_date = find_closest_date(read_data_full,float(user_min_date))
    try: # In case non-interactive terminal, set to default:
        user_max_date = def_max_date
        user_max_date = my_input(f"End date?   [{timeout} seconds to respond],[Enter for default {def_max_date}]", timeout)
    except:
        print(f"setting to default of {def_min_date} unless overridden by begindate/enddate arguments.")
    if user_max_date in ["N","n",None]:
        user_max_date = def_max_date
    try: 
        float(user_max_date)
    except ValueError:
        if verbose: 
            print(f"could not convert string to float; setting to default of {def_max_date} unless overridden by begindate/enddate arguments.")
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
def build_plot(dataframe, filetype, plot_by="datetime", plot_quantbox=True, q_low=0.005, q_hi=0.995, plot_lines=True, open_file=False, show_plot=False,color_by_data=False):
    
    fig = go.Figure() # Set up the figure
    miny,maxy = get_quantiles(dataframe, q_low, q_hi)
    # Date limits as MJD:
    minx, maxx = dataframe['MJD'].min(),dataframe['MJD'].max()
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
            sysex(0)

        fig.add_trace(
            go.Scattergl(
                x=x_box, 
                y=y_box,
                text=f"Quantile range: {q_low} - {q_hi}",
                name=f'99% Range',
                fill="toself",
                fillcolor='rgba(256,220,150,0.2)',
                line={
                    "color":"rgba(256,100,0,0.7)"
                }
            )
        )
    if plot_lines:
        data_trace_mode = "lines+markers"
    else: 
        data_trace_mode = "markers"
    if color_by_data:
        datapt_mode = {
            "color": dataframe['Data'],
            "cmin": dataframe['Data'].min(),
            "cmax": dataframe['Data'].max(),
            "colorscale": "rainbow"
        }
    else:
        datapt_mode = {"color": 'rgba(0,100,256,0.99)'}

    fig.add_trace(
        go.Scattergl(
            x=dataframe['datetime'],
            y=dataframe['Data'],
            mode=data_trace_mode,
            line={"shape": 'hv', "color": 'rgba(200,100,100,0.25)'},
            marker=datapt_mode,
            name=f'{filetype}')
        )
    fig.update_layout( # Give proper axis labels, title, and choose font
        title={
            "text": f"{filetype}: {desciption} Monitor",
            "x": 0.5,
            'xanchor': 'center',
        },
        xaxis_title=plot_by.upper(),
        yaxis_title=filetype,
        font={
            "family":"serif",
            "size":24,
            "color":"black"
        }
        )
    if filetype in zoom_df['Mnemonic'].values:
        fig.update_layout(
            yaxis=dict(
                range=[
                    zoom_df.loc[zoom_df['Mnemonic']==filetype].min_y.item(),
                    zoom_df.loc[zoom_df['Mnemonic']==filetype].max_y.item()
                ]
            )
        )
    
    new_filename = f'{plots_dir}/{filetype}_{minx:.1f}to{maxx:.1f}.html'
    if show_plot:
        fig.show()
    fig.write_html(new_filename)
    if verbose:
        print(f"Saved to: {new_filename}")

    if open_file:
        subprocess.run(['open', new_filename], check=True)
# %%
# OSM1 position
osm1_dict = { #Sets conversion of the position to the graph height
    'Unknown': -3,
    '--': -2,
    'AbMvFail': -1,
    'RelMvReq': 0,
    'G140L': 1,
    'G130M': 2,
    'G160M': 3,
    'NCM1': 4,
    'NCM1FLAT': 5,
}
osm1_color_dict = { #Sets conversion of the position to the graph color
    'Unknown': "darkgray",
    '--': "gray",
    'AbMvFail': "red",
    'RelMvReq': "black",
    'G140L': "chocolate",
    'G130M': "darkblue",
    'G160M': "crimson",
    'NCM1': "gold",
    'NCM1FLAT': "darkgoldenrod",
}
# OSM2 position
osm2_dict = { #Sets conversion of the position to the graph height
    'Unknown': -3,
    '--': -2,
    'AbMvFail': -1,
    'RelMvReq': 0,
    'G230L': 1,
    'G185M': 2,
    'G225M': 3,
    'G285M': 4,
    'TA1Image': 5,
    'TA1Brght': 6
}
osm2_color_dict = { #Sets conversion of the position to the graph color
    'Unknown': "darkgray",
    '--': "gray",
    'AbMvFail': "red",
    'RelMvReq': "black",
    'G230L': "chocolate",
    'G185M': "darkblue",
    'G225M': "darkgreen",
    'G285M': "crimson",
    'TA1Image': "gold",
    'TA1Brght': "darkgoldenrod"
}

# %%
def build_osm_plot(dataframe, filetype, plot_by="datetime", plot_lines=True, valdict=osm1_dict, colordict=osm1_color_dict):
    """
    The OSM plots have text values, not numerical values of their Data column 
    We'll need to translate this to a number, then plot with that number labeled as the position string
    """
    name_conversion_df = pd.DataFrame.from_dict(data = valdict, orient= 'index')
    color_conversion_df = pd.DataFrame.from_dict(data = colordict, orient= 'index')
    
    # TODO: These two lines raise the SettingWithCopyWarning
    #   But I CAN'T figure out why! As a result, briefly suppress the warning
    #   https://stackoverflow.com/questions/42105859/pandas-map-to-a-new-column-settingwithcopywarning/42106022
    pd.options.mode.chained_assignment = None 
    dataframe.loc[:,'Datanum'] = dataframe.Data.map(lambda x: valdict[x])
    dataframe.loc[:,'Graphcolor'] = dataframe.Data.map(lambda x: colordict[x])
    pd.options.mode.chained_assignment = "warn" 
    
    # Date limits as MJD:
    minx, maxx = dataframe['MJD'].min(),dataframe['MJD'].max()
    # Or Date limits as datetimes
    minx_dt,maxx_dt = Time(minx,format='mjd').to_datetime(), Time(maxx,format='mjd').to_datetime() 
    
    fig = go.Figure() # Set up the figure
    fig.add_trace(
        go.Scattergl(
            x=dataframe['datetime'],
            y=dataframe['Datanum'],
            mode='markers+lines',
            name = filetype,
            line={"shape": 'hv', "color": 'rgba(150,190,190,0.35)'},
            marker = dict(
                color = dataframe['Graphcolor'],
            )
        )
    )
    fig.update_layout(
        yaxis=dict(
            tickmode = 'array',
            tickvals = list(valdict.values()),
            ticktext = list(valdict.keys())
        ),
        title={
                "text": f"{filetype}: {desciption} Monitor",
                "x": 0.5,
                'xanchor': 'center',
        },
        xaxis_title=plot_by.upper(),
        yaxis_title=f"OSM State {filetype}",
        font={
            "family":"serif",
            "size":24,
            "color":"black"
        }
    )
    fig['layout']['yaxis']['showgrid'] = False
    # fig.show()
    new_filename = f'{osm_plots_dir}/{filetype}_{minx:.1f}to{maxx:.1f}.html'
    fig.write_html(new_filename)
    if verbose:
        print(f"Saved to: {new_filename}")

# %%

for item_num, filetype in enumerate(file_dict.keys()):
    # RUN CONDITIONALS (to limit which files are run)
    # while item_num < 2: # If you want to limit to the first N files
    if filetype in selected_filetypes: # or if want to limit to a set of filetypes
        
        try:
            desciption = mnemon_df.loc[mnemon_df['Mnemonic']==filetype]['Description'].values[0]
            print(f"{item_num+1}/{len(file_dict.keys())}: Running for {bcolors.BOLD}{filetype}: {bcolors.UNDERLINE}{desciption}{bcolors.ENDC} monitor")
            read_data_full = read_data(telemetry_dir/filetype)

            # Set the default min/max date of the plot to the last year since most recent point:
            def_max_date = read_data_full['MJD'].iloc[-1]
            def_min_date = def_max_date - 365.25
        
        # Get the start/end dates in the correct format:
            if args.ask_user:
                print("Asking the user for dates:\n")
                # Solicit the min/max date of the plot from the user, with defaults if no numerical date detected
                mindex, user_min_date, maxdex, user_max_date = ask_user_dates(verbose = verbose, timeout=TIMEOUT)
            else:
                mindex, user_min_date = find_closest_date(read_data_full,def_min_date)
                maxdex, user_max_date = find_closest_date(read_data_full,def_max_date)
            
            if args.begindate: # If user specifies begin/end dates, these will OVERRIDE all other dates
                mindex, user_min_date = find_closest_date(read_data_full,args.begindate)
            if args.enddate:
                maxdex, user_max_date = find_closest_date(read_data_full,args.enddate)

        # Actually cut the data to that size:
            trimmed_data = read_data_full[mindex:maxdex]
            
        # Do we want to color the datapoints by the data (y) value?
            if filetype in color_by_data_list:
                color_by_data = True
            else:
                color_by_data = False
            # Do we want to skip plotting the quantile box?
            if filetype in skip_quantbox_list:
                plot_quantbox = False
            else:
                plot_quantbox = True

            build_plot(dataframe=trimmed_data, filetype=filetype ,plot_by="datetime", plot_quantbox=plot_quantbox, q_low=0.005, q_hi=0.995, plot_lines=True, open_file=False, show_plot=False, color_by_data=color_by_data)
        except Exception as ex:
            if 'OSM1' in filetype:
                build_osm_plot(dataframe=trimmed_data,filetype=filetype,plot_by="datetime")
            elif 'OSM2' in filetype:
                build_osm_plot(dataframe=trimmed_data,filetype=filetype,plot_by="datetime", valdict=osm2_dict, colordict=osm2_color_dict)
            else:
                print(f"Something went wrong on file: {filetype}")
                print(ex)
# %% 
# To calculate an individual value:
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


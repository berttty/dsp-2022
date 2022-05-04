from pandas.core.common import flatten
import pandas as pd
import numpy as np


#TODO: refactor

def get_stats_for_column(rides, column):
    all_values = pd.concat(list(map(lambda x: x.ride_df, rides)), sort=False)[column].dropna().tolist() 
    return {"min":min(all_values), "mean":sum(all_values)/len(all_values), "max":max(all_values)}

def get_duration_stats(rides):
    all_durations = get_duration_values(rides)
    return {"mean": sum(all_durations)/len(all_durations), "sum": sum(all_durations), "min": min(all_durations), "max": max(all_durations)}

def get_duration_values(rides):
    return list(map(lambda x: x.duration, rides))

def get_min_max_values(rides, columns):
    result = {}
    for column in columns:
        all_values = list(flatten(list(map(lambda x: x.ride_df[column].dropna().tolist(), rides))))
        result[column] = {"min":min(all_values), "mean":sum(all_values)/len(all_values), "max":max(all_values)}
    return result

# TODO find out where the max values are
def get_frequencies_for_columns(rides, verbose=False):
    result = {}
    
    for column in rides[0].ride_df.columns:
        all_values = []
        for ride in rides:
            if column in ride.ride_df.columns:
                freq = ride.frequency_for_column(column)
                if freq is not None:
                    all_values.append(freq)
            else:
                if verbose:
                    print("Column: {} doesn't exist for ride {}, existing columns{}".format(column, ride.title, ride.ride_df.columns))
        hist, edges = np.histogram(all_values)

        result[column] = {"median":np.median(all_values), "min":min(all_values), "mean":np.mean(all_values), "max":max(all_values), "hist_values":hist, "hist_edges": edges}
    return result


def get_phonelocation_distribution(rides):
    result = {}
    for ride in rides:
        key = ride.phone_location.name
        if result.get(key) is None:
            result[key] = 1
        else:
            result[key] += 1

    for key in result.keys():
        result[key] /= len(rides)
    return result


def get_bicycle_distribution(rides):
    result = {}
    for ride in rides:
        key = ride.bicycle_type.name
        if result.get(key) is None:
            result[key] = 1
        else:
            result[key] += 1

    for key in result.keys():
        result[key] /= len(rides)
    return result

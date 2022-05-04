from scipy import signal
from sklearn.decomposition import FastICA
import pandas as pd


def clip_frames(input, start = 0, end = 0):
    return input[start:end]

def scale(input, max_value):
    return input / max_value


def fast_ICA(input, segments_count):
    transformer = FastICA(n_components=segments_count)
    return transformer.fit_transform(input)


def signal_filter(input, fs, cut_off, type):
    fs = fs  # sampling frequency
    fc = cut_off  # Cut-off frequency of the filter
    w = fc / (fs / 2)  # Normalize the frequency
    b, a = signal.butter(5, w, type)
    return signal.filtfilt(b, a, input)


def low_pass_filter(values, fs, cut_off=2):
    input = values
    return signal_filter(input, fs, cut_off, "low")


def high_pass_filter(values, fs, cut_off=1):
    input = values
    return signal_filter(input, fs, cut_off, "high")

# FIXME:


def band_pass_filter(values, fs, lower_bound, upper_bound):
    input = values
    input = signal_filter(input, fs, upper_bound, "high")
    return signal_filter(input, fs, lower_bound, "low")

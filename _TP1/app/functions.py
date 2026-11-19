import pandas as pd

def configure_pandas():
    pd.set_option("display.float_format", "{:.2f}".format)

def print_title(title):
    print()
    print("-" * len(title))
    print(title)
    print("-" * len(title))

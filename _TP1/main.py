from extract.extract import run_bronze
from transform.transform import run_silver
from load.load import run_gold

def main():
    df_bronze = run_bronze()
    df_silver = run_silver(df_bronze)
    run_gold(df_silver)

if __name__ == "__main__":
    main()

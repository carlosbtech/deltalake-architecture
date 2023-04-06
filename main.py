from src.bronze import data_transfer_landing_to_bronze as dtlb
from src.silver import data_process_bronze_to_silver as dpbts
from src.gold import data_process_silver_to_gold as dpstg

if __name__ == '__main__':
    dtlb()
    dpbts()
    dpstg()
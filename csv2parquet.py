import dask.dataframe as dd
from datetime import datetime


def convert_csv_to_parquet(filename, sample_percent):
    start_time = datetime.now()
    print('file name: ' + filename + '.csv')
    print('sample and converstion start time: ' + str(start_time))

    df = dd.read_csv(filename + '.csv', assume_missing=True)

    print('total row count: ' + str(len(df)))

    if sample_percent < 100:
        df = df.sample(sample_percent/100)
        print('sampled total row count: ' + str(len(df)))

    print(df.head())

    df.to_parquet(filename + '.parquet', engine='pyarrow', compression='snappy')

    print()
    print('-----------------------------------------')
    print('sample and converstion complete')
    print('start time: ' + str(start_time))
    endTime = datetime.now()
    print('end time: ' + str(endTime))
    print('elapsed time: ' + str(endTime - start_time))
    print('-----------------------------------------')


if __name__ == '__main__':
    convert_csv_to_parquet('Most-Recent-Cohorts-Scorecard-Elements', 100)

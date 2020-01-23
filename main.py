from numpy import cov
import geopandas as gpd
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
import random

# random for the moment
crime_rate = []
for i in range(0, 52):
    crime_rate.append(random.random())

usa = gpd.read_file('./maps/states.shp')


def insert_to_shapely_file(array, name):
    for index, row in usa.iterrows():
        usa.loc[index, name] = array[index]


def visualize_usa_by_column(column):
    label = {'label': f' USA map en fonction de {column}', 'orientation': "horizontal"}
    fig, ax = plt.subplots(figsize=(30, 30))

    usa[0:51].plot(column=column, legend=True, legend_kwds=label, ax=ax, alpha=0.3, cmap='OrRd', edgecolor='g')


def calculate_and_plot_correlation(data1, data2, corr_name):
    covariance = cov(data1, data2)

    # Pearson's correlation coefficient = covariance(X, Y) / (stdv(X) * stdv(Y)) (data with Gaussian distribution)
    corr, _ = pearsonr(data1, data2)

    fig = plt.figure()
    fig.suptitle(f'Correlation with {corr_name} is {round(corr, 2)}', fontsize=14, fontweight='bold')

    plt.scatter(data1, data2)
    plt.show()


insert_to_shapely_file(crime_rate, 'CRIME_RATE')

# random for the moment
poverty_rate = []
for i in range(0, 52):
    poverty_rate.append(7 * random.random())

insert_to_shapely_file(poverty_rate, 'POVERTY_RATE')

visualize_usa_by_column('POVERTY_RATE')

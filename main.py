from numpy import cov
import geopandas as gpd
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
import csv

usa = gpd.read_file('./maps/states.shp')


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


def insert_to_shapely_file_from_csv(file, state_format, rate_column):
    with open(file, newline='') as csv_file:

        reader = csv.DictReader(csv_file)
        data = {}

        for row in reader:
            state = row['state']
            rate = row[rate_column]

            data[state] = rate
            column = rate_column

        i = 0
        for abr in usa.get(state_format):

            try:
                usa.loc[i, column.upper()] = float(data[abr])
            except:
                usa.loc[i, column.upper()] = 0

            i = i + 1


insert_to_shapely_file_from_csv('data/incidents.csv', 'STATE_ABBR', 'event_count')

insert_to_shapely_file_from_csv('data/gun_ownership.csv', 'STATE_NAME', 'gun_owner_rate')

insert_to_shapely_file_from_csv('data/police_spending.csv', 'STATE_NAME', 'police_spending')

insert_to_shapely_file_from_csv('data/graduation.csv', 'STATE_NAME', 'grad_rate')

insert_to_shapely_file_from_csv('data/education_spending.csv', 'STATE_NAME', 'cost_per_student')

visualize_usa_by_column('EVENT_COUNT')

visualize_usa_by_column('GUN_OWNER_RATE')

visualize_usa_by_column('POLICE_SPENDING')

visualize_usa_by_column('COST_PER_STUDENT')

visualize_usa_by_column('GRAD_RATE')

visualize_usa_by_column('COST_PER_STUDENT')

calculate_and_plot_correlation(usa.EVENT_COUNT, usa.GUN_OWNER_RATE, 'Crime and un ownership correlation')

calculate_and_plot_correlation(usa.EVENT_COUNT, usa.POLICE_SPENDING, 'Crime and police spending correlation')

calculate_and_plot_correlation(usa.EVENT_COUNT, usa.GRAD_RATE, 'Crime and graduation rate correlation')

calculate_and_plot_correlation(usa.EVENT_COUNT, usa.COST_PER_STUDENT, 'Crime and cost per student correlation')

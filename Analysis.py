# Written by Nick Moir and Matthew Gottfried

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats as stats
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import seaborn as sns


db_path = r"C:\Users\bfkiw\PyCharmProjects\DSFinal\output.db"

conn = sqlite3.connect(db_path)
tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
tables = pd.read_sql_query(tables_query, conn)
print("Tables in the database:", tables)

table_name = "Final Project Data"
query = f'SELECT * FROM "{table_name}";'
data = pd.read_sql_query(query, conn)
conn.close()


selected_columns = ['Percent of People without High School Degree', 'Median Household Income (in Thousands)', 'Incarceration Rate per 100,000']

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)
pd.set_option("display.max_colwidth", None)
print(data[selected_columns].describe())

for col in selected_columns:
    if col in data.columns and data[col].dtype == 'object':
        data[col] = data[col].str.replace(',', '').astype(float)

correlation_matrix = data[selected_columns].corr()

x_variables = ['Percent of People without High School Degree', 'Median Household Income (in Thousands)']
y = data['Incarceration Rate per 100,000']

results = {}
for x_var in x_variables:
    X = data[[x_var]]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    correlation_coefficient, p_value = stats.pearsonr(X.squeeze(), y)

    results[x_var] = {
        'Mean Squared Error': mse,
        'R^2 Score': r2,
        'Coefficient': model.coef_[0],
        'Pearson Correlation': correlation_coefficient,
        'P-Value': p_value
    }

for x_var, metrics in results.items():
    print(f"\nLinear Regression for {x_var} vs. Incarceration Rate per 100,000:")
    print(f"  Mean Squared Error: {metrics['Mean Squared Error']}")
    print(f"  R^2 Score: {metrics['R^2 Score']}")
    print(f"  Coefficient: {metrics['Coefficient']}")
    print(f"  Pearson Correlation: {metrics['Pearson Correlation']}")
    print(f"  P-Value: {metrics['P-Value']}")

for x_var in x_variables:
    joint_plot = sns.jointplot(
        data=data,
        x=x_var,
        y=y,
        kind="reg",
        height=10,
        marginal_kws={"bins": 20, "fill": True}
    )

    plt.subplots_adjust(top=0.93)
    joint_plot.fig.suptitle(
        f"{x_var} vs. Incarceration Rate per 100,000",
        fontsize=16
    )

    plt.show()

plt.figure(figsize=(12, 10))
sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", fmt=".2f", annot_kws={"size": 10})
plt.title("Correlation Heatmap", fontsize=16)
plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=0)
plt.tight_layout()
plt.show()




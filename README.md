
# Complex SQL Query Project using Databricks

[![CI Pipeline](https://github.com/nogibjj/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/actions/workflows/main.yaml/badge.svg)](https://github.com/nogibjj/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/actions/workflows/main.yaml)

## Project Overview
This project involves designing and implementing a complex SQL query using Databricks, covering aspects such as joins, aggregation, and sorting. The query will provide insights into the given datasets by combining multiple tables and performing aggregations to highlight relevant patterns. The results will be documented and explained, focusing on functionality and clarity. Src/lib python file contains necessary function to connect Databricks, extracting the csv file and pushing into the respective Databricks table.

## Datasets
We are using two datasets hosted on the project repository:
- **Olympic Dictionary**: [CSV file link](https://github.com/nogibjj/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/blob/af712012aa34bc3f30c124f85848d9417420dc66/data/olympic_dictionary.csv)
- **Olympic Summer**: [CSV file link](https://github.com/nogibjj/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/blob/af712012aa34bc3f30c124f85848d9417420dc66/data/olympic_summer.csv)

These datasets contain information about Olympic athletes, events, and medals, which will be used to form the basis of our query.

## SQL Query
The SQL query designed for this project combines data from the `Olympic Dictionary` and `Olympic Summer` datasets. It performs the following steps:

1. **Join Operation**: 
   - We perform left join the medals information with the country information based on the Code of the countries.
   
2. **Aggregation**: 
   - The query aggregates the total number of medals won by each athlete and calculates the sum of medals grouped by country.

3. **Sorting**: 
   - The results are sorted in descending order by the total number of medals, allowing us to rank  countries based on their achievements.

```sql
SELECT 
country.Country
,summer.Medal
,COUNT(*) as cnt
from ids706_data_engineering.default.olympicsummer_jk645 summer
LEFT JOIN ids706_data_engineering.default.olympicdictionary_jk645 country ON summer.Country = country.Code
GROUP BY country.Country, summer.Medal
HAVING Country IS NOT NULL
ORDER BY cnt DESC
```

### Explanation
- **JOIN**: We join the `olympicdictionary_jk645` dataset with the `olympicsummer_jk645` using `code`. This allows us to connect country information details with their sportments in respective performances in the Summer Olympics.
- **GROUP BY**: The query groups the results by `Country`, `Medal`, to count the total number of each medals won by each country.
- **Having**: Removing unnecessary NULL values from the result.
- **ORDER BY**: The query sorts the final output by the `TotalMedals` (called cnt) in descending order, showing countries with the highest number of medals first.

### Expected Results
The query is expected to return:

- **CountryName**: The country name.
- **MedalType**: Type of medal won
- **TotalMedals**: The total number of medals won by each country.


### Results Images

- **Terminal Output**: ![Terminal Output](https://github.com/nogibjj/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/blob/af712012aa34bc3f30c124f85848d9417420dc66/img/Screenshot%202024-10-13%20at%206.10.40%E2%80%AFPM.png)


- **Databrick Console Output**: ![Databrick Console Output](https://github.com/nogibjj/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/blob/af712012aa34bc3f30c124f85848d9417420dc66/img/Screenshot%202024-10-13%20at%206.25.57%E2%80%AFPM.png)


SELECT 
country.Country
,summer.Medal
,COUNT(*) as cnt
from ids706_data_engineering.default.olympicsummer_jk645 summer
LEFT JOIN ids706_data_engineering.default.olympicdictionary_jk645 country ON summer.Country = country.Code
GROUP BY country.Country, summer.Medal
HAVING Country IS NOT NULL
ORDER BY cnt DESC


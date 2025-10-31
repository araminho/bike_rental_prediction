SELECT w.id, w.temperature as temp, w.apparent_temperature atemp, w.windspeed, w.precipitation, w.humidity as hum, w.weather_type,
b.season, b.yr, b.mnth, b.day, b.hr, b.weekday, b.classic_bike, b.electric_bike, b.casual, b.registered, b.cnt
FROM weather_data w
LEFT JOIN bike_data b ON w.id = b.id
ORDER BY id
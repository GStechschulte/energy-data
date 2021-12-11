CREATE TABLE IF NOT EXISTS energy.demand
(
	date_ts date NOT NULL,
	MWh numeric DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS energy.dev
(
	date_ts date NOT NULL,
	MWh numeric DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS energy.generation
(
	date_ts date NOT NULL,
	hydro_MWh numeric DEFAULT NULL,
    solar_MWh numeric DEFAULT NULL,
    wind_MWh numeric DEFAULT NULL
);

-- To be completed once Ari transforms the weather data :)
/*
CREATE TABLE IF NOT EXISTS energy.weather
(
	date_ts date NOT NULL,
	wind_speed numeric DEFAULT NULL,
    cloud_cover numeric DEFAULT NULL,
    precipitation numeric DEFAULT NULL,
);
/*


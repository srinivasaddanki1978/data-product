-- Validates that all forecast dates are in the future.
SELECT forecast_date
FROM {{ ref('int__cost_forecast') }}
WHERE forecast_date <= CURRENT_DATE()

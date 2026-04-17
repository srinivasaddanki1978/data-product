-- Validates that no forecast predictions are negative.
SELECT forecast_date, predicted_total_cost
FROM {{ ref('int__cost_forecast') }}
WHERE predicted_total_cost < 0

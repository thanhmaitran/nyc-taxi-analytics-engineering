{# Descriptions come from the TLC Yellow Trips data dictionary (2025-03-18). #}

select
    payment_type_code as payment_type_key,
    payment_type_code,
    payment_type_description
from (
    values
    (0, 'Flex Fare trip'),
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip'),
    (-1, 'Missing / not provided')
) as payment_types (payment_type_code, payment_type_description)  -- noqa: AL05

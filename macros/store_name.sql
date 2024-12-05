{% macro store_name(variable, alias=True) %}
    {%- set processed_variable = "LOWER(TRIM(" ~ variable ~ "))" -%}
    CASE
        WHEN {{ processed_variable }} IN ('amazon.com.mx', 'mx', 'mexico', 'a1am78c64um0y8') THEN 'Mexico'
        WHEN {{ processed_variable }} IN ('amazon.com.br', 'br', 'brazil', 'a2q3y263d00kwc') THEN 'Brazil'
        WHEN {{ processed_variable }} IN ('amazon.co.uk', 'gb', 'uk', 'united kingdom', 'si uk prod marketplace', 'a1f83g8c2aro7p') THEN 'United Kingdom'
        WHEN {{ processed_variable }} IN ('amazon.com', 'usd', 'us', 'united states', 'atvpdkikx0der') THEN 'United States'
        WHEN {{ processed_variable }} IN ('amazon.ca', 'cad', 'ca', 'canada', 'si ca prod marketplace', 'a2euq1wtgctbg2', 'shopify.ca') THEN 'Canada'
        WHEN {{ processed_variable }} IN ('amazon.in', 'inr', 'in', 'india', 'a21tjruun4kgv') THEN 'India'
        WHEN {{ processed_variable }} IN ('amazon.fr', 'fr', 'france', 'a13v1ib3viyzzh') THEN 'France'
        WHEN {{ processed_variable }} IN ('amazon.es', 'es', 'spain', 'a1rkkupihcs9hs') THEN 'Spain'
        WHEN {{ processed_variable }} IN ('amazon.de', 'de', 'germany', 'a1pa6795ukmfr9') THEN 'Germany'
        WHEN {{ processed_variable }} IN ('amazon.it', 'it', 'italy', 'apj6jra9ng5v4') THEN 'Italy'
        WHEN {{ processed_variable }} IN ('amazon.com.au', 'au', 'australia', 'a39ibj37trp1c6') THEN 'Australia'
        WHEN {{ processed_variable }} IN ('amazon.ae', 'ae', 'united arab emirates', 'a2vigq35rcs4ug') THEN 'United Arab Emirates'
        WHEN {{ processed_variable }} IN ('amazon.sg', 'sg', 'singapore', 'a19vau5u5o7rus') THEN 'Singapore'
        WHEN {{ processed_variable }} IN ('amazon.co.jp', 'jp', 'japan', 'a1vc38t7yxb528') THEN 'Japan'
        WHEN {{ processed_variable }} IN ('amazon.nl', 'nl', 'netherlands', 'a1805izsgtt6hs') THEN 'Netherlands'
        WHEN {{ processed_variable }} IN ('amazon.se', 'se', 'sweden', 'a2nodrkzp88zb9') THEN 'Sweden'
        WHEN {{ processed_variable }} IN ('amazon.co.za', 'za', 'south africa', 'ae08wj6yknbmc') THEN 'South Africa'
        WHEN {{ processed_variable }} IN ('amazon.pl', 'pl', 'poland', 'a1c3sozrarq6r3') THEN 'Poland'
        WHEN {{ processed_variable }} IN ('amazon.eg', 'eg', 'egypt', 'arbp9ooshtchu') THEN 'Egypt'
        WHEN {{ processed_variable }} IN ('amazon.com.tr', 'tr', 'turkey', 'a33avaj2pdy3ev') THEN 'Turkey'
        WHEN {{ processed_variable }} IN ('amazon.sa', 'sa', 'saudi arabia', 'a17e79c6d8dwnp') THEN 'Saudi Arabia'
        WHEN {{ processed_variable }} IN ('amazon.com.co', 'co', 'colombia', 'a571pqmxg6f4h') THEN 'Colombia'
        WHEN {{ processed_variable }} IN ('amazon.com.ar', 'ar', 'argentina', 'a1vj6wm4pf18w8') THEN 'Argentina'
        ELSE {{ variable }}
    END
    {%- if alias %}
        AS store_name
    {%- endif -%}
{% endmacro %}

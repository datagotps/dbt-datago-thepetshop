{% macro dev_date_filter(date_column, date_ranges=none) %}
    {#
    Macro to apply development date filtering based on dbt variables.
    
    Usage:
        {{ dev_date_filter('order_date') }}
        {{ dev_date_filter('posting_date', [{'start': '2025-01-01', 'end': '2025-09-30'}]) }}
    
    To enable dev mode:
        dbt run --vars 'dev_mode: true'
    
    To use custom date ranges:
        dbt run --vars '{"dev_mode": true, "dev_date_ranges": [{"start": "2025-01-01", "end": "2025-03-31"}]}'
    #}
    
    {% if var('dev_mode', false) %}
        
        {# Use custom date ranges if provided, otherwise use default ranges #}
        {% set ranges = date_ranges or var('dev_date_ranges', [
            {'start': '2025-01-01', 'end': '2025-09-30'},
            {'start': '2024-12-01', 'end': '2024-12-31'},
            {'start': '2024-01-01', 'end': '2024-01-31'}
        ]) %}
        
        AND (
            {% for range in ranges %}
                {{ date_column }} BETWEEN '{{ range.start }}' AND '{{ range.end }}'
                {% if not loop.last %} OR {% endif %}
            {% endfor %}
        )
    {% endif %}
    
{% endmacro %}

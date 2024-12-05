{% macro enabled_sources(model_type) %}

    {% set enabled_sources_list = [] %}
    
    {% if var("AMAZONSBADS") %}
        {% do enabled_sources_list.append("amazonsb") %}
    {% endif %}

    {% if var("AMAZONSPADS") %}
        {% do enabled_sources_list.append("amazonsp") %}
    {% endif %}

    {% if var("AMAZONSDADS") %}
        {% do enabled_sources_list.append("amazonsd") %}
    {% endif %}

    {% if var("FACEBOOK") %}
        {% do enabled_sources_list.append("facebookads") %}
    {% endif %}

    {% if var("GOOGLEADS") %}
        {% do enabled_sources_list.append("googleads") %}
    {% endif %}

    {% if var("BINGADS") %}
        {% do enabled_sources_list.append("bingads") %}
    {% endif %}

    {% if var("PINTERESTADS") %}
        {% do enabled_sources_list.append("pinterestads") %}
    {% endif %}

    {% if var("KLAVIYO3") %}
        {% do enabled_sources_list.append("klaviyo") %}
    {% endif %}

    {% if var("GA4") %}
        {% do enabled_sources_list.append("googleanalytics") %}
    {% endif %}

    {% if var("AMAZONSELLER") %}
        {% do enabled_sources_list.append("amazonseller") %}
    {% endif %}

    {% if var("AMAZONVENDOR") %}
        {% do enabled_sources_list.append("amazonvendor") %}
    {% endif %}

    {% if var("SHOPIFYV2") %}
        {% do enabled_sources_list.append("shopify") %}
    {% endif %}

    {{ return(enabled_sources_list) }}
{% endmacro %}

{% macro filter_prefix_relations(relations, filter_prefix_list) %}
    {% set filtered_relations = [] %}
    {% for relation in relations %}
        {% for prefix in filter_prefix_list %}
            {% if prefix in relation.identifier %} {% do filtered_relations.append(relation) %} {% endif %}
        {% endfor %}
    {% endfor %}
    {{ return(filtered_relations) }}
{% endmacro %}

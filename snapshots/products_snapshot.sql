{% snapshot products_snapshot %}

{{
    config(
      target_database='dev',
      target_schema='snapshots',
      unique_key='product_id',
      strategy='check',
      check_cols=['cost'],
    )
}}

select * from {{ source('ecommerce', 'raw_products') }}

{% endsnapshot %}
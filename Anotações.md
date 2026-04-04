# Selecionando Dados

### Orders (Pedidos)
- order_id -> provavelmente vai acabar sendo o que vou usar para fazer os merges nas tabelas
- customer_id -> Caso a localidade esteja atrelada ao cliente e não ao pedido, vou utilizar pro merge da localização
- order_status
- order_purchase_timestamp
- order_approved_at
- order_carrier_date
- order_delivered_costumer_date
- order_estimated_delivery_date

O meu dataframe final deverá conter só o necessário pra geração das KPI´s, então as colunas serão:

- Order_id
- customer_id
- order_status
- aprove_intervall (diferença entre o horário de compra e o de aprovação)
- deliver_intervall (diferença entre o carrier e delivered)
- estimated_delivery (diferença entre o approved e o estimated)

### Items

- order_id
- order_item_id
- product_id
- seller_id
- shipping_limit_date
- price
- freight_value

Nesse caso eu devo fazer uma interação entre o Orders e Items, já que dentro desse dataframe eu tenho o limite de data de envio
pro determinado pedido:

- shipping_acurracy

- ### Payments

- order_id
- payment_sequential -> Diferentes entradas de métodos de pagamento
- payment_type
- payment_installments -> Parcelas
- payment_value
No caso de Voucher ele tem muitas entradas de pagamento (max 29) acho uma boa fazer a soma do valor por order_id, manter o payment_type e o maximo em payment_sequential. Não importa tanto o histórico de vouchers que o cliente utilizou, mas é bom saber qual foi o valor do produto e o metodo utilizado. Além disso, os vouchers contam o valor de frete também.

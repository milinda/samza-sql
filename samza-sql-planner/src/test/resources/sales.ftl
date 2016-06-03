{
  version: '1.0',
  defaultSchema: 'SALES',
  schemas: [{
    name: 'SALES',
    type: 'custom',
    factory: 'org.apache.samza.sql.schema.SamzaSQLSchemaFactory',
    operand: {
      schemaregistry: 'http://localhost:8081',
      kafkabrokers: '${brokers}',
      zookeeper: '${zkConnecitonStr}'
    },
    tables: [{
      name: 'Orders',
      type: 'custom',
      factory: 'org.apache.samza.sql.schema.SamzaSQLTableFactory',
      stream: {
        stream: true
      },
      operand: {
        keytype: 'string',
        messageschema: {
          "type": "record",
          "namespace": "com.example",
          "name": "Orders",
          "fields": [{
            "name": "orderId",
            "type": "int"
          }, {
            "name": "productId",
            "type": "int"
          }, {
            "name": "rowtime",
            "type": "long"
          }, {
            "name": "units",
            "type": "int"
          }]
        }
      }
    }, {
      name: 'filtered',
      type: 'custom',
      factory: 'org.apache.samza.sql.schema.SamzaSQLTableFactory',
      stream: {
        stream: true
      },
      operand: {
        keytype: 'string',
        messageschema: {
          "type": "record",
          "namespace": "com.example",
          "name": "Filtered",
          "fields": [{
            "name": "orderId",
            "type": "int"
          }, {
            "name": "productId",
            "type": "int"
          }, {
            "name": "rowtime",
            "type": "long"
          }, {
            "name": "units",
            "type": "int"
          }]
        }
      }
    }
    ]
  }]
}

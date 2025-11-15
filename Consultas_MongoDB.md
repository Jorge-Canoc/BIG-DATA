# Código ejecutado dentro de la aplicación MongoDB Compass, instalada localmente.

# El dataset para el ejercicio se encuentra en https://www.kaggle.com/datasets/carrie1/ecommerce-data 

# Insertar documento
{
  "NumeroFactura": 581588,
  "CodigoProducto": "XYZ001",
  "Descripcion": "NUEVO PRODUCTO",
  "Cantidad": 10,
  "FechaFactura": "01/01/2024 10:00",
  "PrecioUnitario": 15.99,
  "IDCliente": 99999,
  "Pais": "Colombia"
}


# Seleccion
{Pais: "Colombia"}


# Consulta en el pais United Kingdom compras con cantidad mayor a 1000
{
  Pais: "United Kingdom",
  Cantidad: { $gte: 1000 }
}


# Consulta de compras con cantidades mayores a 200 o menores a 10
{
  $or: [
    { Cantidad: { $gt: 200 } },
    { Cantidad: { $lt: 10 } }
  ]
}


# Consulta para cliente 17850 para compras con cantidad menor a 5
{ 
  IDCliente: 17850, 
  Cantidad: { $lte: 5 } 
}



# Compras en el país Francia con precio unitario mayor a 15 dólares.
{
  Pais: 'France',
	PrecioUnitario: {$gte: 15.0}
}


# Consulta agregada para conocer la cantidad de compras y elementos de un cliente
[
  {
    $match:
      /**
       * seleccion de un cliente
       */
      {
        IDCliente: 17850
      }
  },
  {
    $group:
      /**
       * _valores totales del cliente.
       */
      {
        _id: 17850,
        totalCompras: {
          $sum: 1
        },
        totalCantidad: {
          $sum: "$Cantidad"
        },
        totalValorUnitario: {
          $sum: "$PrecioUnitario"
        }
      }
  }
]
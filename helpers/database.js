function objectToSqlInsertArticulo(tableName, obj) {
  // Elimina las propiedades con valores undefined
  const cleanedObj = Object.fromEntries(
    Object.entries(obj).filter(([key, value]) => value !== undefined)
  );

  const columns = Object.keys(cleanedObj).join(", ");

  const values = Object.values(cleanedObj)
    .map((value) => {
      if (
        format_iso_date(value) != false &&
        value &&
        typeof value === "object"
      ) {
        return `TIMESTAMP '${format_iso_date(value)}'`;

        // return `CONVERT(DATE,SUBSTRING('${value}',0, 11),105)`
      } else if (value === null) {
        return "NULL";
      } else if (typeof value === "string") {
        return `'${value.replace(/'/g, "''")}'`;
      } else {
        return value;
      }
    })
    .join(", ");

  const sql = `INSERT INTO ${tableName} (${columns}) VALUES (${values});`;

  return sql;
}
function objectToSqlUpdateArticulo(tableName, obj, condition) {
  // Elimina las propiedades con valores undefined
  const cleanedObj = Object.fromEntries(
    Object.entries(obj).filter(([key, value]) => value !== undefined)
  );

  const setClause = Object.entries(cleanedObj)
    .map(([key, value]) => {
      if (
        format_iso_date(value) != false &&
        value &&
        typeof value === "object"
      ) {
        return `${key} = TIMESTAMP '${format_iso_date(value)}'`;
      } else if (value === null) {
        return `${key} = NULL`;
      } else if (typeof value === "string") {
        return `${key} = '${value.replace(/'/g, "''")}'`;
      } else {
        return `${key} = ${value}`;
      }
    })
    .join(", ");

  const sql = `UPDATE ${tableName} SET ${setClause} WHERE ${condition};`;

  return sql;
}
function format_iso_date(dateString) {
  const isoDateRegExp = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$/;

  const date = new Date(dateString);

  if (!isNaN(date.getTime())) {
    const formattedDate = date.toISOString().replace("T", " ").replace("Z", "");
    return formattedDate;
  } else if (
    typeof dateString === "string" &&
    dateString.match(isoDateRegExp)
  ) {
    const formattedDate = dateString.replace("T", " ").replace("Z", "");
    return formattedDate;
  }

  return false;
}

async function compararArraysDeObjetos(array1, array2, id, auxObj) {
  const diferencias = [];

  for (const objeto1 of array1) {
    const objeto2 = array2.find((o) => o[id] === objeto1[id + "_MICROSIP"]);

    if (objeto2) {
      const diff = await compararObjetos(objeto1, objeto2, id, auxObj);
      if (Object.keys(diff).length > 0) {
        diferencias.push(diff);
      }
    }
  }

  return diferencias;
}

async function compararObjetos(objeto1, objeto2, id_key, auxObj) {
  const keys1 = Object.keys(objeto1);
  const keys2 = Object.keys(objeto2);

  const keys = Array.from(new Set([...keys1, ...keys2]));

  const diff = {};

  for (const key of keys) {
    let valor1 = objeto1[key];
    let valor2 = objeto2[key];

    if (key === "NOTAS_COMPRAS" && valor1) {
      valor1 = await getBlobData(valor1);
    }
    if (key === "NOTAS_COMPRAS" && valor2) {
      valor2 = await getBlobData(valor2);
    }

    if (key === "NOTAS_VENTAS" && valor1) {
      valor1 = await getBlobData(valor1);
    }
    if (key === "NOTAS_VENTAS" && valor2) {
      valor2 = await getBlobData(valor2);
    }

    if (
      valor1 != valor2 &&
      !key.includes("FECHA") &&
      !key.includes("MICROSIP") &&
      key != id_key &&
      !key.toLowerCase().includes("_id")
    ) {
      diff[key] = valor2;
      diff[id_key] = objeto1[id_key];
      diff[id_key + "_MICROSIP"] = objeto1[id_key + "_MICROSIP"];
    } else if (
      key.toLowerCase().includes("_id") &&
      !key.includes("MICROSIP") &&
      key != id_key &&
      auxObj &&
      key != "CONJUNTO_SUCURSALES_ID"
    ) {
      const jcontrolValue = auxObj[key].find(
        (elm) => elm[key + "_MICROSIP"] == valor2
      )?.[key];
      if (jcontrolValue != valor1) {
        //console.log({id_key, jcontrolValue, valor1})
        diff[key] = jcontrolValue;
        diff[id_key] = objeto1[id_key];
        diff[id_key + "_MICROSIP"] = objeto1[id_key + "_MICROSIP"];
      }
    }
  }

  return diff;
}
const getBlobData = (blob) => {
  return new Promise((res, rej) => {
    blob(function (err, name, eventEmitter) {
      eventEmitter.on("data", function (chunk) {
        res(chunk.toString("utf8"));
      });
    });
  });
};

module.exports = {
  objectToSqlInsertArticulo,
  objectToSqlUpdateArticulo,
  compararArraysDeObjetos,
  getBlobData,
};

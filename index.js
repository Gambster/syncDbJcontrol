const Firebird = require("node-firebird");
const msDatabase = require("./msDatabase");
const jcDatabase = require("./jcDatabase");
const {
  objectToSqlInsertArticulo,
  objectToSqlUpdateArticulo,
  compararArraysDeObjetos,
  getBlobData,
} = require("./helpers/database");

const main = async () => {
  try {
    const dbMicrosip = await connect(msDatabase);
    const dbJcontrol = await connect(jcDatabase);

    const gruposLineasMicrosip = await query(
      dbMicrosip,
      `SELECT * FROM GRUPOS_LINEAS;`
    );

    const gruposLineasJcontrol = await query(
      dbJcontrol,
      `SELECT * FROM GRUPOS_LINEAS;`
    );

    const missing_grupos_lineas = gruposLineasMicrosip.filter(
      (elm) =>
        !gruposLineasJcontrol.some(
          (elm2) => elm.GRUPO_LINEA_ID == elm2.GRUPO_LINEA_ID
        )
    );

    const dif_grupos_lineas = await compararArraysDeObjetos(
      gruposLineasJcontrol,
      gruposLineasMicrosip,
      "GRUPO_LINEA_ID"
    );
    for (let diff of dif_grupos_lineas) {
      const query_str = await objectToSqlUpdateArticulo(
        "GRUPOS_LINEAS",
        diff,
        `GRUPO_LINEA_ID=${diff.GRUPO_LINEA_ID}`
      );
      await query(dbJcontrol, query_str);
    }

    for (let missing of missing_grupos_lineas) {
      const query_str = objectToSqlInsertArticulo("grupos_lineas", missing);
      await query(dbJcontrol, query_str);
    }
    const lineasMicrosip = await query(
      dbMicrosip,
      `SELECT * FROM LINEAS_ARTICULOS;`
    );

    const lineasJcontrol = await query(
      dbJcontrol,
      `SELECT * FROM LINEAS_ARTICULOS;`
    );

    const missing_lineas = lineasMicrosip.filter(
      (elm) =>
        !lineasJcontrol.some(
          (elm2) => elm.LINEA_ARTICULO_ID == elm2.LINEA_ARTICULO_ID
        )
    );

    const dif_lineas = await compararArraysDeObjetos(
      lineasJcontrol,
      lineasMicrosip,
      "LINEA_ARTICULO_ID"
    );

    for (let diff of dif_lineas) {
      const query_str = await objectToSqlUpdateArticulo(
        "LINEAS_ARTICULOS",
        diff,
        `LINEA_ARTICULO_ID=${diff.LINEA_ARTICULO_ID}`
      );
      await query(dbJcontrol, query_str);
    }

    for (let missing of missing_lineas) {
      const query_str = objectToSqlInsertArticulo("lineas_articulos", missing);
      await query(dbJcontrol, query_str);
    }

    const articulosMicrosip = await query(
      dbMicrosip,
      `SELECT * FROM ARTICULOS;`
    );
    const articulosJcontrol = await query(
      dbJcontrol,
      `SELECT * FROM ARTICULOS;`
    );

    const missing_articulos = articulosMicrosip.filter(
      (elm) =>
        !articulosJcontrol.some((elm2) => elm.ARTICULO_ID == elm2.ARTICULO_ID)
    );

    const dif_articulos = await compararArraysDeObjetos(
      articulosJcontrol,
      articulosMicrosip,
      "ARTICULO_ID"
    );

    for (let diff of dif_articulos) {
      const query_str = await objectToSqlUpdateArticulo(
        "ARTICULOS",
        diff,
        `ARTICULO_ID=${diff.ARTICULO_ID}`
      );
      await query(dbJcontrol, query_str);
    }

    for (let missing of missing_articulos) {
      if (missing.NOTAS_VENTAS)
        missing = {
          ...missing,
          ["NOTAS_VENTAS"]: await getBlobData(missing.NOTAS_VENTAS),
        };
      if (missing.NOTAS_COMPRAS) {
        missing = {
          ...missing,
          ["NOTAS_COMPRAS"]: await getBlobData(missing.NOTAS_COMPRAS),
        };
      }
      const query_str = objectToSqlInsertArticulo("articulos", missing);
      await query(dbJcontrol, query_str);
    }
    const rolesClavesMicrosip = await query(
      dbMicrosip,
      `SELECT * FROM ROLES_CLAVES_ARTICULOS;`
    );
    const rolesClavesJcontrol = await query(
      dbJcontrol,
      `SELECT * FROM ROLES_CLAVES_ARTICULOS;`
    );

    const dif_rolesClaves = await compararArraysDeObjetos(
      rolesClavesJcontrol,
      rolesClavesMicrosip,
      "ROL_CLAVE_ART_ID"
    );

    for (let diff of dif_rolesClaves) {
      const query_str = await objectToSqlUpdateArticulo(
        "ROLES_CLAVES_ARTICULOS",
        diff,
        `ROL_CLAVE_ART_ID=${diff.ROL_CLAVE_ART_ID}`
      );
      await query(dbJcontrol, query_str);
    }

    const missing_rolesClaves = rolesClavesMicrosip.filter(
      (elm) =>
        !rolesClavesJcontrol.some(
          (elm2) => elm.ROL_CLAVE_ART_ID == elm2.ROL_CLAVE_ART_ID
        )
    );

    for (let missing of missing_rolesClaves) {
      const query_str = objectToSqlInsertArticulo(
        "ROLES_CLAVES_ARTICULOS",
        missing
      );
      await query(dbJcontrol, query_str);
    }

    const ClavesMicrosip = await query(
      dbMicrosip,
      `SELECT * FROM CLAVES_ARTICULOS;`
    );
    const ClavesJcontrol = await query(
      dbJcontrol,
      `SELECT * FROM CLAVES_ARTICULOS;`
    );

    const missing_claves = ClavesMicrosip.filter(
      (elm) =>
        !ClavesJcontrol.some(
          (elm2) => elm.CLAVE_ARTICULO_ID == elm2.CLAVE_ARTICULO_ID
        )
    );
    const dif_Claves = await compararArraysDeObjetos(
      ClavesJcontrol,
      ClavesMicrosip,
      "CLAVE_ARTICULO_ID"
    );

    for (let diff of dif_Claves) {
      const query_str = await objectToSqlUpdateArticulo(
        "CLAVES_ARTICULOS",
        diff,
        `CLAVE_ARTICULO_ID=${diff.CLAVE_ARTICULO_ID}`
      );
      await query(dbJcontrol, query_str);
    }

    for (let missing of missing_claves) {
      const query_str = objectToSqlInsertArticulo("CLAVES_ARTICULOS", missing);
      await query(dbJcontrol, query_str);
    }
    console.log("done");
  } catch (error) {
    console.log({ error });
  }
};

const connect = async (database) => {
  for (let i = 0; i < 3; i++) {
    try {
      const db = await new Promise((resolve, reject) => {
        Firebird.attach(database, (err, db) => {
          if (err) {
            reject(err);
          } else {
            resolve(db);
          }
        });
      });

      return db;
    } catch (error) {
      console.error(`Intento ${i + 1} fallido:`, error);
      if (i === 3 - 1) {
        throw new Error(
          `Se alcanzó el número máximo de intentos (${3}). Error: ${error}`
        );
      }
    }
  }
};

const query = (db, query) => {
  return new Promise((res, rej) => {
    db.query(query, (err, result) => {
      if (err) {
        console.log({ query });
        rej(err);
      }
      res(result);
    });
  });
};
main();

setInterval(main, 5000);

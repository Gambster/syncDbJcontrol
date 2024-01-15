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
    console.log("empieza");
    const dbMicrosip = await connect(msDatabase);
    const dbJcontrol = await connect(jcDatabase);
    console.log("cons");

    /********************************* INICIA GRUPOS LINEAS *********************************/
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
          (elm2) => elm.GRUPO_LINEA_ID == elm2.GRUPO_LINEA_ID_MICROSIP
        )
    );
    const del_grupos_lineas = gruposLineasJcontrol.filter(
      (elm) =>
        !gruposLineasMicrosip.some(
          (elm2) => elm.GRUPO_LINEA_ID_MICROSIP == elm2.GRUPO_LINEA_ID
        )
    );

    const dif_grupos_lineas = await compararArraysDeObjetos(
      gruposLineasJcontrol,
      gruposLineasMicrosip,
      "GRUPO_LINEA_ID"
    );

    await createRows(
      missing_grupos_lineas,
      "grupos_lineas",
      dbJcontrol,
      "GRUPO_LINEA_ID"
    );
    await updateRows(
      dif_grupos_lineas,
      "GRUPOS_LINEAS",
      "GRUPO_LINEA_ID",
      dbJcontrol
    );
    await deleteRows(
      del_grupos_lineas,
      "GRUPOS_LINEAS",
      "GRUPO_LINEA_ID",
      dbJcontrol
    );

    console.log("termina grupos");

    /********************************* INICIA LINEAS_ARTICULOS *********************************/

    const lineasMicrosip = await query(
      dbMicrosip,
      `SELECT * FROM LINEAS_ARTICULOS;`
    );

    const lineasJcontrol = await query(
      dbJcontrol,
      `SELECT * FROM LINEAS_ARTICULOS;`
    );

    let missing_lineas = lineasMicrosip.filter(
      (elm) =>
        !lineasJcontrol.some(
          (elm2) => elm.LINEA_ARTICULO_ID == elm2.LINEA_ARTICULO_ID_MICROSIP
        )
    );

    missing_lineas = missing_lineas.map((elm) => ({
      ...elm,
      ["GRUPO_LINEA_ID"]: gruposLineasJcontrol.find(
        (elm2) => elm.GRUPO_LINEA_ID == elm2.GRUPO_LINEA_ID_MICROSIP
      )?.GRUPO_LINEA_ID,
    }));

    const del_lineas = lineasJcontrol.filter(
      (elm) =>
        !lineasMicrosip.some(
          (elm2) => elm.LINEA_ARTICULO_ID_MICROSIP == elm2.LINEA_ARTICULO_ID
        )
    );

    const dif_lineas = await compararArraysDeObjetos(
      lineasJcontrol,
      lineasMicrosip,
      "LINEA_ARTICULO_ID",
      {
        ["GRUPO_LINEA_ID"]: gruposLineasJcontrol,
      }
    );


    await createRows(
      missing_lineas,
      "lineas_articulos",
      dbJcontrol,
      "LINEA_ARTICULO_ID"
    );
    await updateRows(
      dif_lineas,
      "LINEAS_ARTICULOS",
      "LINEA_ARTICULO_ID",
      dbJcontrol
    );
    await deleteRows(
      del_lineas,
      "LINEAS_ARTICULOS",
      "LINEA_ARTICULO_ID",
      dbJcontrol
    );

    console.log("termina lineas");

    // /********************************* INICIA ARTICULOS *********************************/
    const articulosMicrosip = await query(
      dbMicrosip,
      `SELECT * FROM ARTICULOS;`
    );
    const articulosJcontrol = await query(
      dbJcontrol,
      `SELECT * FROM ARTICULOS;`
    );

    let missing_articulos = articulosMicrosip.filter(
      (elm) =>
        !articulosJcontrol.some(
          (elm2) => elm.ARTICULO_ID == elm2.ARTICULO_ID_MICROSIP
        )
    );

    missing_articulos = missing_articulos.map((elm) => ({
      ...elm,
      ["LINEA_ARTICULO_ID"]: lineasJcontrol.find(
        (elm2) => elm.LINEA_ARTICULO_ID == elm2.LINEA_ARTICULO_ID_MICROSIP
      )?.LINEA_ARTICULO_ID,
    }));

    const dif_articulos = await compararArraysDeObjetos(
      articulosJcontrol,
      articulosMicrosip,
      "ARTICULO_ID",
      {
        ["LINEA_ARTICULO_ID"]: lineasJcontrol,
      }
    );

    const del_articulos = articulosJcontrol.filter(
      (elm) =>
        !articulosMicrosip.some(
          (elm2) => elm.ARTICULO_ID_MICROSIP == elm2.ARTICULO_ID
        )
    );

    await createRows(missing_articulos, "articulos", dbJcontrol, "ARTICULO_ID");
    await updateRows(dif_articulos, "ARTICULOS", "ARTICULO_ID", dbJcontrol);
    await deleteRows(del_articulos, "ARTICULOS", "ARTICULO_ID", dbJcontrol);
    console.log("termina articulos");

    // /********************** INICIA ROLES CLAVES ARTICULOS *********************************/
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

    const missing_rolesClaves = rolesClavesMicrosip.filter(
      (elm) =>
        !rolesClavesJcontrol.some(
          (elm2) => elm.ROL_CLAVE_ART_ID == elm2.ROL_CLAVE_ART_ID_MICROSIP
        )
    );

    const del_rolesClaves = rolesClavesJcontrol.filter(
      (elm) =>
        !rolesClavesMicrosip.some(
          (elm2) => elm.ROL_CLAVE_ART_ID_MICROSIP == elm2.ROL_CLAVE_ART_ID
        )
    );

    await createRows(
      missing_rolesClaves,
      "ROLES_CLAVES_ARTICULOS",
      dbJcontrol,
      "ROL_CLAVE_ART_ID"
    );
    await updateRows(
      dif_rolesClaves,
      "ROLES_CLAVES_ARTICULOS",
      "ROL_CLAVE_ART_ID",
      dbJcontrol
    );
    await deleteRows(
      del_rolesClaves,
      "ROLES_CLAVES_ARTICULOS",
      "ROL_CLAVE_ART_ID",
      dbJcontrol
    );
    console.log("termina roles c");
    // /********************** INICIA CLAVES ARTICULOS *********************************/
    const ClavesMicrosip = await query(
      dbMicrosip,
      `SELECT * FROM CLAVES_ARTICULOS;`
    );
    const ClavesJcontrol = await query(
      dbJcontrol,
      `SELECT * FROM CLAVES_ARTICULOS;`
    );

    let missing_claves = ClavesMicrosip.filter(
      (elm) =>
        !ClavesJcontrol.some(
          (elm2) => elm.CLAVE_ARTICULO_ID == elm2.CLAVE_ARTICULO_ID_MICROSIP
        )
    );

    missing_claves = missing_claves.map((elm) => ({
      ...elm,
      ["ARTICULO_ID"]: articulosJcontrol.find(
        (elm2) => elm.ARTICULO_ID == elm2.ARTICULO_ID_MICROSIP
      )?.ARTICULO_ID,
      ["ROL_CLAVE_ART_ID"]: rolesClavesJcontrol.find(
        (elm2) => elm.ROL_CLAVE_ART_ID == elm2.ROL_CLAVE_ART_ID_MICROSIP
      )?.ROL_CLAVE_ART_ID,
    }));

    const dif_Claves = await compararArraysDeObjetos(
      ClavesJcontrol,
      ClavesMicrosip,
      "CLAVE_ARTICULO_ID",
      {
        ["ARTICULO_ID"]: articulosJcontrol,
        ["ROL_CLAVE_ART_ID"]: rolesClavesJcontrol,
      }
    );
    const del_Claves = ClavesJcontrol.filter(
      (elm) =>
        !ClavesMicrosip.some(
          (elm2) => elm.CLAVE_ARTICULO_ID_MICROSIP == elm2.CLAVE_ARTICULO_ID
        )
    );

    await createRows(
      missing_claves,
      "CLAVES_ARTICULOS",
      dbJcontrol,
      "CLAVE_ARTICULO_ID"
    );
    await updateRows(
      dif_Claves,
      "CLAVES_ARTICULOS",
      "CLAVE_ARTICULO_ID",
      dbJcontrol
    );
    await deleteRows(
      del_Claves,
      "CLAVES_ARTICULOS",
      "CLAVE_ARTICULO_ID",
      dbJcontrol
    );

    // /********************** INICIA TIPOS IMPUESTOS *********************************/
    const tiposImpMicrosip = await query(
      dbMicrosip,
      `SELECT * FROM TIPOS_IMPUESTOS;`
    );
    const tiposImpJcontrol = await query(
      dbJcontrol,
      `SELECT * FROM TIPOS_IMPUESTOS;`
    );

    const missing_tipos_imp = tiposImpMicrosip.filter(
      (elm) =>
        !tiposImpJcontrol.some(
          (elm2) => elm.TIPO_IMPTO_ID == elm2.TIPO_IMPTO_ID_MICROSIP
        )
    );

    const dif_tipos_imp = await compararArraysDeObjetos(
      tiposImpJcontrol,
      tiposImpMicrosip,
      "TIPO_IMPTO_ID"
    );
    const del_tipos_imp = tiposImpJcontrol.filter(
      (elm) =>
        !tiposImpMicrosip.some(
          (elm2) => elm.TIPO_IMPTO_ID_MICROSIP == elm2.TIPO_IMPTO_ID
        )
    );

    await createRows(
      missing_tipos_imp,
      "TIPOS_IMPUESTOS",
      dbJcontrol,
      "TIPO_IMPTO_ID"
    );
    await updateRows(
      dif_tipos_imp,
      "TIPOS_IMPUESTOS",
      "TIPO_IMPTO_ID",
      dbJcontrol
    );
    await deleteRows(
      del_tipos_imp,
      "TIPOS_IMPUESTOS",
      "TIPO_IMPTO_ID",
      dbJcontrol
    );

    // /********************** INICIA IMPUESTOS *********************************/
    const impuestosMicrosip = await query(
      dbMicrosip,
      `SELECT * FROM IMPUESTOS;`
    );
    const impuestosJcontrol = await query(
      dbJcontrol,
      `SELECT * FROM IMPUESTOS;`
    );

    let missing_impuestos = impuestosMicrosip.filter(
      (elm) =>
        !impuestosJcontrol.some(
          (elm2) => elm.IMPUESTO_ID == elm2.IMPUESTO_ID_MICROSIP
        )
    );

    missing_impuestos = missing_impuestos.map((elm) => ({
      ...elm,
      ["TIPO_IMPTO_ID"]: tiposImpJcontrol.find(
        (elm2) => elm.TIPO_IMPTO_ID == elm2.TIPO_IMPTO_ID_MICROSIP
      )?.TIPO_IMPTO_ID,
    }));

    const dif_impuestos = await compararArraysDeObjetos(
      impuestosJcontrol,
      impuestosMicrosip,
      "IMPUESTO_ID",
      {
        ["TIPO_IMPTO_ID"]: tiposImpJcontrol,
      }
    );
    const del_impuestos = impuestosJcontrol.filter(
      (elm) =>
        !impuestosMicrosip.some(
          (elm2) => elm.IMPUESTO_ID_MICROSIP == elm2.IMPUESTO_ID
        )
    );

    await createRows(missing_impuestos, "IMPUESTOS", dbJcontrol, "IMPUESTO_ID");
    await updateRows(dif_impuestos, "IMPUESTOS", "IMPUESTO_ID", dbJcontrol);
    await deleteRows(del_impuestos, "IMPUESTOS", "IMPUESTO_ID", dbJcontrol);

    // /********************** INICIA IMPUESTOS ARTICULOS *********************************/
    const impuestosArtMicrosip = await query(
      dbMicrosip,
      `SELECT * FROM IMPUESTOS_ARTICULOS;`
    );
    const impuestosARTJcontrol = await query(
      dbJcontrol,
      `SELECT * FROM IMPUESTOS_ARTICULOS;`
    );

    let missing_impuestos_art = impuestosArtMicrosip.filter(
      (elm) =>
        !impuestosARTJcontrol.some(
          (elm2) => elm.IMPUESTO_ART_ID == elm2.IMPUESTO_ART_ID_MICROSIP
        )
    );

    missing_impuestos_art = missing_impuestos_art.map((elm) => ({
      ...elm,
      ["ARTICULO_ID"]: articulosJcontrol.find(
        (elm2) => elm.ARTICULO_ID == elm2.ARTICULO_ID_MICROSIP
      )?.ARTICULO_ID,
      ["IMPUESTO_ID"]: impuestosJcontrol.find(
        (elm2) => elm.IMPUESTO_ID == elm2.IMPUESTO_ID_MICROSIP
      )?.IMPUESTO_ID,
    }));

    const dif_impuestos_art = await compararArraysDeObjetos(
      impuestosARTJcontrol,
      impuestosArtMicrosip,
      "IMPUESTO_ART_ID",
      {
        ["ARTICULO_ID"]: articulosJcontrol,
        ["IMPUESTO_ID"]: impuestosJcontrol,
      }
    );
    const del_impuestos_art = impuestosARTJcontrol.filter(
      (elm) =>
        !impuestosArtMicrosip.some(
          (elm2) => elm.IMPUESTO_ART_ID_MICROSIP == elm2.IMPUESTO_ART_ID
        )
    );

    await createRows(
      missing_impuestos_art,
      "IMPUESTOS_ARTICULOS",
      dbJcontrol,
      "IMPUESTO_ART_ID"
    );
    await updateRows(
      dif_impuestos_art,
      "IMPUESTOS_ARTICULOS",
      "IMPUESTO_ART_ID",
      dbJcontrol
    );
    await deleteRows(
      del_impuestos_art,
      "IMPUESTOS_ARTICULOS",
      "IMPUESTO_ART_ID",
      dbJcontrol
    );

    // /********************** INICIA USUARIOS *********************************/

    const usuariosMicrosip = await query(
      dbMicrosip,
      `SELECT * FROM SEC$USERS;`
    );
    const usuariosJcontrol = await query(
      dbJcontrol,
      `SELECT * FROM SEC$USERS;`
    );

    const missing_usuarios = usuariosMicrosip.filter(
      (elm) =>
        !usuariosJcontrol.some(
          (elm2) => elm.SEC$USER_NAME.trim() == elm2.SEC$USER_NAME.trim()
        )
    );

    for (const user of missing_usuarios) {
      await query(
        dbJcontrol,`CREATE USER ${user.SEC$USER_NAME} PASSWORD 'JC0ntr0L' FIRSTNAME '${user.SEC$FIRST_NAME}' LASTNAME '${user.SEC$LAST_NAME}' GRANT ADMIN ROLE;`
      );
    }

    const dif_usuarios = await compararArraysDeObjetos(
      usuariosJcontrol,
      usuariosMicrosip,
      "SEC$USER_NAME"
    );

    for (const user of dif_usuarios) {
      await query(
        dbJcontrol,
        `ALTER USER ${user.SEC$USER_NAME} FIRSTNAME '${user.SEC$FIRST_NAME}' LASTNAME '${user.SEC$LAST_NAME}';`
      );
    }

    const del_usuarios = usuariosJcontrol.filter(
      (elm) =>
        !usuariosMicrosip.some(
          (elm2) => elm.SEC$USER_NAME.trim() == elm2.SEC$USER_NAME.trim()
        )
    );

    for (const user of del_usuarios) {
      await query(dbJcontrol, `DROP USER ${user.SEC$USER_NAME.toUpperCase()};`);
    }

    /********************************************* FINISH **********************************************/

    console.log("termina todo");
    await dbMicrosip.detach();
    await dbJcontrol.detach();
    console.log({
      missing_articulos,
      missing_claves,
      missing_grupos_lineas,
      missing_lineas,
      missing_rolesClaves,
      missing_tipos_imp,
      missing_impuestos,
      missing_impuestos_art,
      missing_usuarios,
      dif_Claves,
      dif_articulos,
      dif_grupos_lineas,
      dif_lineas,
      dif_rolesClaves,
      dif_tipos_imp,
      dif_impuestos,
      dif_impuestos_art,
      dif_usuarios,
      del_Claves,
      del_articulos,
      del_grupos_lineas,
      del_lineas,
      del_rolesClaves,
      del_tipos_imp,
      del_impuestos,
      del_impuestos_art,
      del_usuarios,
    });
    console.log("done");
  } catch (error) {
    console.log({ error });
  }
};

const deleteRows = async (rows, tableName, keyName, dbJcontrol) => {
  for (const row of rows) {
    if (tableName.toLowerCase() == "articulos") {
      await query(
        dbJcontrol,
        `DELETE FROM CLAVES_ARTICULOS WHERE ${keyName} = ${row[keyName]};`
      );
    }
    await query(
      dbJcontrol,
      `DELETE FROM ${tableName} WHERE ${keyName + "_MICROSIP"} = ${
        row[keyName + "_MICROSIP"]
      };`
    );
  }
};

const updateRows = async (rows, tableName, keyName, dbJcontrol) => {
  for (let diff of rows) {
    const query_str = await objectToSqlUpdateArticulo(
      tableName,
      diff,
      `${keyName + "_MICROSIP"}=${diff[keyName + "_MICROSIP"]}`
    );
    await query(dbJcontrol, query_str);
  }
};

const createRows = async (rows, tableName, dbJcontrol, id_keyName) => {
  for (let missing of rows) {
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

    missing[id_keyName + "_MICROSIP"] = missing[id_keyName];
    missing[id_keyName] =
      (
        await query(
          dbJcontrol,
          `SELECT COALESCE(MAX(${id_keyName}), 0) AS MaximoID FROM ${tableName};`
        )
      )[0].MAXIMOID + 1;
    const query_str = objectToSqlInsertArticulo(tableName, missing);
    await query(dbJcontrol, query_str);
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

setInterval(main, 300000);

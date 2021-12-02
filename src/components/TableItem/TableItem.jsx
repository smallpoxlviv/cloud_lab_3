import styles from "./TableItem.module.css";
import React from 'react'
import ReactDOM from 'react-dom'

const TableItem = ({
  sensor_id,
  name,
  settlement,
  latitude,
  longtitude,
  water_level,
  measurement_date
}) => {

  return (
    <div className={styles.container}>
      <h3 className={styles.number}>{sensor_id}</h3>
      <h3>{name}</h3>
      <h3>{settlement}</h3>
      <h3>{latitude}</h3>
      <h3>{longtitude}</h3>
      <h3>{water_level}</h3>
      <h3>{measurement_date}</h3>
    </div>
  );
};

export default TableItem;

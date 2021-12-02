import TableItem from "../TableItem/TableItem";
import styles from "./Table.module.css";
import React from 'react'
import ReactDOM from 'react-dom'

const Table = ({ parcels }) => {
  console.log(parcels)
  return (
    <div className={styles.container}>
      <div className={styles.headersContainer}>
        <h3>Sensor id</h3>
        <h3>Name</h3>
        <h3>Settlement</h3>
        <h3>Latitude</h3>
        <h3>Longtitude</h3>
        <h3>Water level</h3>
        <h3>Measurement date</h3>
      </div>
      <div className={styles.itemContainer}>
        {
        parcels.map((x) => (
          <TableItem {...x} key={x.id} />
        ))}
      </div>
    </div>
  );
};

export default Table;

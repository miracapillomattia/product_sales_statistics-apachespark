package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class TreStatistiche {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("StatisticaVendite")
                .master("local")
                .getOrCreate();

        																						// Imposta il percorso del file CSV
        String csvFile = "C:\\Users\\U-19\\eclipse-workspace\\ApcheSparkTreStatistiche\\eserc2.csv";
																											
																											        // Leggi il file CSV e crea un DataFrame
        Dataset<Row> data = sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(csvFile);

        																									// Calcola il totale delle vendite per ciascuna data
        Dataset<Row> totalSalesPerDate = data.groupBy("data").agg(sum("vendita").alias("totaleVendite"));

        																								// Trova il prodotto più venduto per ciascuna data
        Dataset<Row> mostSoldProductPerDate = data.groupBy("data", "prodotto")
                .agg(sum("quantita").alias("quantitaTotale"))
                .orderBy(desc("quantitaTotale"))
                .groupBy("data")
                .agg(first("prodotto").alias("prodottoPiuVenduto"));

        																							// Calcola la quantità totale venduta per ciascun prodotto
        Dataset<Row> totalQuantitySoldPerProduct = data.groupBy("prodotto")
                .agg(sum("quantita").alias("quantitaTotale"));

        																									// Mostra i risultati
//        System.out.println("Totale vendite per ciascuna data:");
//        totalSalesPerDate.show();

        System.out.println("Prodotto più venduto per ciascuna data:");
        mostSoldProductPerDate.show();

//        System.out.println("Quantità totale venduta per ciascun prodotto:");
//        totalQuantitySoldPerProduct.show();

        sparkSession.stop();
    }
}

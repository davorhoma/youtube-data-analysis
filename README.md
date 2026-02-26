# YouTube Big Data Analytics

Projekat se bavi analizom velikih skupova podataka sa platforme YouTube, uz podršku za **paketnu obradu (batch processing)** i **obradu tokova podataka (stream processing)**. Sistem je izgrađen na distribuiranoj arhitekturi koja obuhvata Apache Kafka, Apache Spark, Apache Airflow, Hadoop i MongoDB, a rezultati analiza su vizuelizovani u Metabase-u.

---

## Arhitektura

```
Youtube_producer → Kafka → Spark Structured Streaming → MongoDB → Metabase
                                                                     ↑
HDFS (batch data) → Spark Batch → MongoDB ───────────────────────────┘
                        ↑
                    Airflow (orchestration)
```

**Komponente:**
- **Kafka** – broker za tok podataka u realnom vremenu
- **Apache Spark** – engine za batch i stream obradu
- **Apache Airflow** – orkestracija i zakazivanje DAG-ova
- **Hadoop (HDFS)** – skladište za sirove i transformisane podatke
- **MongoDB** – baza podataka za čuvanje rezultata analiza
- **Metabase** – vizuelizacija i dashboard-i
- **Youtube Producer** – Docker servis koji simulira tok YouTube podataka

---

## Pokretanje klastera

Pozicionirati se u root folder projekta i pokrenuti:

```bash
./scripts/cluster_up.sh
```

Za zaustavljanje klastera:

```bash
./scripts/cluster_down.sh
```

---

## Batch obrada

Batch DAG-ovi se pokreću putem **Airflow GUI-ja** (dostupan na `http://localhost:8080`) ili putem CLI komandi unutar Airflow kontejnera.

Implementirano je **10 analitičkih upita**:

1. Popularnost videa kroz vreme
2. Top 10 kategorija sa najvećim brojem video zapisa
3. YouTube kanali sa najvećim angažovanjem korisnika
4. Promena broja pregleda 10 video zapisa po danu
5. Video zapisi sa najbrže rastućim brojem pregleda
6. Angažovanost korisnika po regionu
7. Najkorišćenije oznake (tags)
8. Najpopularnije kombinacije oznaka
9. Prosečan broj komentara po kategoriji
10. Uticaj dužine trajanja video zapisa na popularnost

---

## Obrada tokova podataka (Streaming)

### 1. Pokretanje producera

Pozicionirati se u folder `Youtube_producer` i pokrenuti:

```bash
docker compose up
```

Producer šalje podatke na Kafka topic u realnom vremenu.

### 2. Pokretanje streaming obrade

U Airflow GUI-ju pokrenuti DAG:

```
youtube_streaming_start
```

Za zaustavljanje streaming obrade pokrenuti DAG:

```
youtube_streaming_stop
```

Implementirano je **5 real-time analitičkih upita** koji se obrađuju u toku prijema podataka:

1. Broj video zapisa sa trajanjem u tri raspona ([0 min, 4 min), [4 min, 20 min), [20 min, +∞])
2. Broj pojava potpuno novih video zapisa na listi 
3. YouTube kanali koji se pojavljuju na više regiona
4. Broj pojava YouTube kanala koji ne postoje u istorijskom skupu podataka
5. Promena trenutnih broja pregleda (maksimalni broj i aritmetička sredina) u odnosu na broj pregleda u istorijskom skupu podataka 

---

## Vizuelizacija

Svi rezultati — i batch i real-time — dostupni su kroz **Metabase** dashboarde. Metabase je dostupan na `http://localhost:3000` nakon pokretanja klastera.

---

## Struktura projekta

```
├── Airflow/          # DAG-ovi, Spark jobovi, konfiguracija
├── Hadoop/           # HDFS konfiguracija
├── Kafka/            # Kafka broker i konfiguracija
├── Metabase/         # Metabase docker compose
├── MongoDB/          # MongoDB docker compose
├── Spark/            # Spark docker compose
├── Youtube_producer/ # Kafka producer za real-time podatke
└── scripts/          # Skripte za pokretanje/zaustavljanje klastera
```
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import networkx as nx
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import re


# ============================================================
# Configuration
# ============================================================

INPUT_FILE = Path("output/netcool_events.jsonl")

OUTPUT_DIRECTORY = Path("output")
CORRELATED_EVENTS_FILE = OUTPUT_DIRECTORY / "correlated_events.jsonl"
GRAPH_FILE = OUTPUT_DIRECTORY / "alarm_dependency_graph.json"
SUMMARY_FILE = OUTPUT_DIRECTORY / "correlation_summary.json"

# DBSCAN settings.
#
# eps is the maximum normalized distance between alarms in one cluster.
# min_samples=2 means at least two similar alarms are needed for a cluster.
DBSCAN_EPS = 0.90
DBSCAN_MIN_SAMPLES = 2

# Isolation Forest settings.
#
# contamination is the expected fraction of anomalies.
# Example: 0.05 means approximately 5% of alarms are expected to be unusual.
ISOLATION_FOREST_CONTAMINATION = 0.05
ISOLATION_FOREST_RANDOM_STATE = 42


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ============================================================
# Utility functions
# ============================================================
import re


def normalize_fault_code(fault_code: str) -> str:
    """
    Normalize noisy fault-code naming before DBSCAN clustering.

    Examples:
    impact-ran-rf-failure -> IMPACT_RAN_RF_FAILURE
    IMPACT RAN RF FAIL -> IMPACT_RAN_RF_FAILURE
    ALARM_RAN_RF_FAILURE -> RAN_RF_FAILURE
    """
    if not isinstance(fault_code, str):
        return "UNKNOWN"

    normalized = fault_code.upper()

    normalized = normalized.replace("-", "_")
    normalized = normalized.replace(" ", "_")
    normalized = normalized.replace("FAIL", "FAILURE")
    normalized = normalized.replace("DEGRADATION", "DEGRADED")
    normalized = normalized.replace("ALARM_", "")
    normalized = re.sub(r"_V\d+$", "", normalized)
    normalized = re.sub(r"_+", "_", normalized)

    return normalized.strip("_")

def ensure_output_directory() -> None:
    """Create output directory if needed."""
    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)


def load_netcool_events() -> pd.DataFrame:
    """
    Read JSON Lines output created by netcool_emulator.py.

    Each line is one alarm event.
    """
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}. "
            "Run netcool_emulator.py first."
        )

    df = pd.read_json(INPUT_FILE, lines=True)

    if df.empty:
        raise ValueError(
            "The Netcool output file exists but contains no alarm events."
        )

    return df


def convert_timestamp_to_seconds(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert timestamp to elapsed seconds from the first alarm.

    This allows DBSCAN to use time as a numerical feature.

    t_i = timestamp_i - min(timestamp)
    """
    df = df.copy()

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        utc=True,
        errors="coerce",
    )

    if df["timestamp"].isna().any():
        raise ValueError(
            "One or more alarm timestamps could not be parsed."
        )

    first_timestamp = df["timestamp"].min()

    df["time_offset_seconds"] = (
        df["timestamp"] - first_timestamp
    ).dt.total_seconds()

    df["normalized_fault_code"] = df["fault_code"].apply(
        normalize_fault_code
    )

    return df


def encode_categorical_features(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Convert categorical alarm fields into numeric values.

    DBSCAN requires numerical features.

    We use:
    - time_offset_seconds
    - fault_code
    - domain
    - severity

    One-hot encoding represents categories mathematically.
    """
    df = df.copy()

    categorical_columns = [
        "normalized_fault_code",
        "domain",
        "severity",
    ]

    encoded = pd.get_dummies(
        df[categorical_columns],
        prefix=categorical_columns,
        dtype=int,
    )

    numeric_features = [
    "time_offset_seconds",
    "ground_truth_delay_seconds",
    "ground_truth_topology_distance",
    ]
    
    for column in numeric_features: 
        if column not in df.columns:
            df[column] = 0


    df["ground_truth_topology_distance"] = (
        df["ground_truth_topology_distance"]
        .fillna(99)
    )
    
    feature_df = pd.concat(
        [
            df[numeric_features].reset_index(drop=True),
            encoded.reset_index(drop=True),
            ],
            axis=1,
    )


    feature_columns = feature_df.columns.tolist()

    return feature_df, feature_columns


def build_dbscan_features(
    df: pd.DataFrame,
) -> tuple[np.ndarray, list[str]]:
    """
    Create standardized features for DBSCAN.

    Standardization uses:

        z = (x - mean(x)) / std(x)

    This prevents time values, which may be large, from dominating
    categorical feature values.
    """
    feature_df, feature_columns = encode_categorical_features(df)

    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(feature_df)

    return scaled_features, feature_columns


def run_dbscan(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Cluster alarms using DBSCAN.

    DBSCAN labels:
    - cluster_id >= 0 : cluster membership
    - cluster_id = -1 : noise / not assigned to a cluster
    """
    df = df.copy()

    features, feature_columns = build_dbscan_features(df)

    model = DBSCAN(
        eps=DBSCAN_EPS,
        min_samples=DBSCAN_MIN_SAMPLES,
    )

    cluster_labels = model.fit_predict(features)

    df["dbscan_cluster_id"] = cluster_labels
    df["dbscan_is_noise"] = df["dbscan_cluster_id"] == -1

    cluster_count = len(
        set(cluster_labels) - {-1}
    )

    noise_count = int(
        (cluster_labels == -1).sum()
    )

    metadata = {
        "algorithm": "DBSCAN",
        "eps": DBSCAN_EPS,
        "min_samples": DBSCAN_MIN_SAMPLES,
        "feature_columns": feature_columns,
        "cluster_count": cluster_count,
        "noise_count": noise_count,
    }

    return df, metadata


def build_isolation_forest_features(
    df: pd.DataFrame,
) -> tuple[np.ndarray, list[str]]:
    """
    Create features for anomaly detection.

    We include:
    - time offset
    - fault code
    - domain
    - severity
    - DBSCAN noise flag
    - whether Netcool could correlate the alarm

    This lets Isolation Forest learn that alarms with unusual
    combinations are potentially anomalous.
    """
    df = df.copy()

    df["netcool_unmatched"] = (
        df["netcool_status"] == "UNMATCHED"
    ).astype(int)

    df["dbscan_noise_numeric"] = (
        df["dbscan_is_noise"]
    ).astype(int)

    categorical_columns = [
        "normalized_fault_code",
        "domain",
        "severity",
        "netcool_status",
    ]

    encoded = pd.get_dummies(
        df[categorical_columns],
        prefix=categorical_columns,
        dtype=int,
    )

    numeric_columns = [
        "time_offset_seconds",
        "netcool_unmatched",
        "dbscan_noise_numeric",
    ]

    feature_df = pd.concat(
        [
            df[numeric_columns].reset_index(drop=True),
            encoded.reset_index(drop=True),
        ],
        axis=1,
    )

    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(feature_df)

    feature_columns = feature_df.columns.tolist()

    return scaled_features, feature_columns


def run_isolation_forest(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Detect anomalies using Isolation Forest.

    Output:
    - isolation_forest_label:
        1  = normal
        -1 = anomaly

    - isolation_forest_score:
        Lower scores generally indicate more unusual events.
    """
    df = df.copy()

    # Isolation Forest needs enough observations to learn a pattern.
    # For very small datasets, use a simple fallback.
    if len(df) < 10:
        df["isolation_forest_label"] = 1
        df["isolation_forest_score"] = 0.0
        df["isolation_forest_is_anomaly"] = False

        metadata = {
            "algorithm": "IsolationForest",
            "status": "SKIPPED_SMALL_DATASET",
            "minimum_required_events": 10,
            "actual_event_count": len(df),
        }

        return df, metadata

    features, feature_columns = build_isolation_forest_features(df)

    model = IsolationForest(
        contamination=ISOLATION_FOREST_CONTAMINATION,
        random_state=ISOLATION_FOREST_RANDOM_STATE,
    )

    labels = model.fit_predict(features)
    scores = model.decision_function(features)

    df["isolation_forest_label"] = labels
    df["isolation_forest_score"] = scores
    df["isolation_forest_is_anomaly"] = labels == -1

    anomaly_count = int((labels == -1).sum())

    metadata = {
        "algorithm": "IsolationForest",
        "contamination": ISOLATION_FOREST_CONTAMINATION,
        "random_state": ISOLATION_FOREST_RANDOM_STATE,
        "feature_columns": feature_columns,
        "anomaly_count": anomaly_count,
    }

    return df, metadata


def determine_ai_root_cause(
    cluster_df: pd.DataFrame,
) -> str | None:
    """
    Select an AI root cause candidate for one DBSCAN cluster.

    Priority:
    1. A Netcool ROOT_CAUSE alarm
    2. A PARENT alarm
    3. The earliest alarm in the cluster

    This is intentionally simple. Later, topology centrality,
    severity, and graph analysis can improve it.
    """
    root_cause_rows = cluster_df[
        cluster_df["netcool_status"] == "ROOT_CAUSE"
    ]

    if not root_cause_rows.empty:
        return root_cause_rows.iloc[0]["event_id"]

    parent_rows = cluster_df[
        cluster_df["alarm_role"] == "PARENT"
    ]

    if not parent_rows.empty:
        return parent_rows.iloc[0]["event_id"]

    earliest_row = cluster_df.sort_values(
        "timestamp"
    ).iloc[0]

    return earliest_row["event_id"]


def assign_cluster_root_causes(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Assign an AI root-cause candidate to each DBSCAN cluster.
    """
    df = df.copy()

    df["ai_root_cause_event_id"] = None

    valid_clusters = sorted(
        cluster_id
        for cluster_id in df["dbscan_cluster_id"].unique()
        if cluster_id != -1
    )

    for cluster_id in valid_clusters:
        cluster_mask = (
            df["dbscan_cluster_id"] == cluster_id
        )

        cluster_df = df[cluster_mask]

        root_cause_event_id = determine_ai_root_cause(
            cluster_df
        )

        df.loc[
            cluster_mask,
            "ai_root_cause_event_id",
        ] = root_cause_event_id

    return df


def build_dependency_graph(
    df: pd.DataFrame,
) -> nx.DiGraph:
    """
    Create a directed dependency graph.

    Graph structure:

        Root Cause Alarm
              │
              ▼
        Correlated Child Alarm

    Additional graph edges:
    - Netcool root cause -> correlated child
    - AI cluster root cause -> cluster members
    - node -> alarm, representing infrastructure origin
    """
    graph = nx.DiGraph()

    for _, alarm in df.iterrows():
        event_id = alarm["event_id"]

        graph.add_node(
            event_id,
            node_type="alarm",
            timestamp=alarm["timestamp"].isoformat(),
            fault_code=alarm["fault_code"],
            domain=alarm["domain"],
            severity=alarm["severity"],
            alarm_role=alarm["alarm_role"],
            netcool_status=alarm["netcool_status"],
            dbscan_cluster_id=int(
                alarm["dbscan_cluster_id"]
            ),
            is_anomaly=bool(
                alarm["isolation_forest_is_anomaly"]
            ),
        )

        infrastructure_node_id = (
            f"INFRA::{alarm['node_id']}"
        )

        graph.add_node(
            infrastructure_node_id,
            node_type="infrastructure",
            node_id=alarm["node_id"],
            node_name=alarm["node_name"],
            domain=alarm["domain"],
        )

        graph.add_edge(
            infrastructure_node_id,
            event_id,
            relationship="GENERATED_ALARM",
        )

    # Add Netcool static-rule dependency edges.
    correlated_children = df[
        df["netcool_status"] == "CORRELATED_CHILD"
    ]

    for _, child_alarm in correlated_children.iterrows():
        root_id = child_alarm[
            "netcool_root_cause_event_id"
        ]

        child_id = child_alarm["event_id"]

        if pd.notna(root_id) and graph.has_node(root_id):
            graph.add_edge(
                root_id,
                child_id,
                relationship="NETCOOL_STATIC_RULE",
            )

    # Add AI/DBSCAN dependency edges.
    clustered_alarms = df[
        df["dbscan_cluster_id"] != -1
    ]

    for _, alarm in clustered_alarms.iterrows():
        root_id = alarm["ai_root_cause_event_id"]
        event_id = alarm["event_id"]

        if (
            pd.notna(root_id)
            and root_id != event_id
            and graph.has_node(root_id)
        ):
            graph.add_edge(
                root_id,
                event_id,
                relationship="AI_DBSCAN_CLUSTER",
            )

    return graph


def export_graph_to_json(graph: nx.DiGraph) -> None:
    """
    Export NetworkX graph in node-link JSON format.

    This format is easy to consume later using:
    - D3.js
    - Cytoscape.js
    - Streamlit graph components
    """
    graph_data = nx.node_link_data(
        graph,
        edges="links",
    )

    with GRAPH_FILE.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            graph_data,
            file,
            indent=2,
            default=str,
        )


def write_correlated_events(df: pd.DataFrame) -> None:
    """
    Save enriched alarms as JSON Lines.
    """
    output_df = df.copy()

    # Convert pandas timestamps to strings for JSON serialization.
    output_df["timestamp"] = output_df[
        "timestamp"
    ].astype(str)

    output_df.to_json(
        CORRELATED_EVENTS_FILE,
        orient="records",
        lines=True,
        date_format="iso",
    )


def create_summary(
    df: pd.DataFrame,
    dbscan_metadata: dict[str, Any],
    isolation_metadata: dict[str, Any],
    graph: nx.DiGraph,
) -> dict[str, Any]:
    """
    Create high-level metrics for later dashboard use.
    """
    return {
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "total_alarms": int(len(df)),
        "dbscan": dbscan_metadata,
        "isolation_forest": isolation_metadata,
        "netcool_status_counts": (
            df["netcool_status"]
            .value_counts()
            .to_dict()
        ),
        "dbscan_cluster_counts": (
            df["dbscan_cluster_id"]
            .value_counts()
            .to_dict()
        ),
        "ai_anomaly_count": int(
            df["isolation_forest_is_anomaly"].sum()
        ),
        "graph_node_count": int(
            graph.number_of_nodes()
        ),
        "graph_edge_count": int(
            graph.number_of_edges()
        ),
    }


def write_summary(summary: dict[str, Any]) -> None:
    """Save dashboard summary metrics."""
    with SUMMARY_FILE.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            summary,
            file,
            indent=2,
            default=str,
        )


# ============================================================
# Main pipeline
# ============================================================

def main() -> None:
    ensure_output_directory()

    logging.info("Loading Netcool events from: %s", INPUT_FILE)

    df = load_netcool_events()

    logging.info("Loaded %d alarm events.", len(df))

    df = convert_timestamp_to_seconds(df)

    logging.info("Running DBSCAN clustering...")

    df, dbscan_metadata = run_dbscan(df)

    logging.info(
        "DBSCAN complete | clusters=%d | noise=%d",
        dbscan_metadata["cluster_count"],
        dbscan_metadata["noise_count"],
    )

    df = assign_cluster_root_causes(df)

    logging.info("Running Isolation Forest anomaly detection...")

    df, isolation_metadata = run_isolation_forest(df)

    if isolation_metadata.get("status") == "SKIPPED_SMALL_DATASET":
        logging.warning(
            "Isolation Forest skipped because fewer than 10 events exist."
        )
    else:
        logging.info(
            "Isolation Forest complete | anomalies=%d",
            isolation_metadata["anomaly_count"],
        )

    logging.info("Building NetworkX dependency graph...")

    graph = build_dependency_graph(df)

    write_correlated_events(df)
    export_graph_to_json(graph)

    summary = create_summary(
        df=df,
        dbscan_metadata=dbscan_metadata,
        isolation_metadata=isolation_metadata,
        graph=graph,
    )

    write_summary(summary)

    logging.info("Correlation engine complete.")
    logging.info(
        "Saved correlated alarms: %s",
        CORRELATED_EVENTS_FILE,
    )
    logging.info(
        "Saved dependency graph: %s",
        GRAPH_FILE,
    )
    logging.info(
        "Saved summary: %s",
        SUMMARY_FILE,
    )


if __name__ == "__main__":
    main()
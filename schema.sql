PRAGMA foreign_keys = ON;

-- =========================
-- DUT
-- =========================
CREATE TABLE IF NOT EXISTS DUT (
    DUT_id INTEGER PRIMARY KEY AUTOINCREMENT,
    wafer TEXT NOT NULL,
    DOE TEXT NOT NULL,
    die INTEGER NOT NULL,
    cage TEXT NOT NULL,
    device TEXT NOT NULL,
    UNIQUE (wafer, DOE, die, cage, device) ON CONFLICT IGNORE);

CREATE INDEX IF NOT EXISTS idx_dut_wafer_die ON DUT (wafer, die);

-- =========================
-- MeasurementSessions
-- =========================
CREATE TABLE IF NOT EXISTS MeasurementSessions (
    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    DUT_id INTEGER NOT NULL,
    session_name TEXT,
    measurement_datetime DATETIME,
    operator TEXT,
    system_version TEXT,
    notes TEXT,
    UNIQUE (session_name),
    FOREIGN KEY (DUT_id) REFERENCES DUT(DUT_id) ON DELETE CASCADE);

CREATE INDEX IF NOT EXISTS idx_session_dut ON MeasurementSessions (DUT_id);
CREATE INDEX IF NOT EXISTS idx_session_datetime ON MeasurementSessions (measurement_datetime);

-- =========================
-- ExperimentalConditions
-- =========================
CREATE TABLE IF NOT EXISTS ExperimentalConditions (
    condition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT,
    FOREIGN KEY (session_id) REFERENCES MeasurementSessions(session_id) ON DELETE CASCADE,
    UNIQUE (session_id, key, unit) ON CONFLICT IGNORE);

CREATE INDEX IF NOT EXISTS idx_condition_session ON ExperimentalConditions (session_id);

-- =========================
-- MeasurementData
-- =========================
CREATE TABLE IF NOT EXISTS MeasurementData (
    data_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    data_type TEXT NOT NULL,     -- e.g. 'spectrum'
    file_path TEXT NOT NULL,
    created_time DATETIME,
    FOREIGN KEY (session_id) REFERENCES MeasurementSessions(session_id) ON DELETE CASCADE);

CREATE INDEX IF NOT EXISTS idx_data_session ON MeasurementData (session_id);
CREATE INDEX IF NOT EXISTS idx_data_type ON MeasurementData (data_type);

-- =========================
-- DataInfo
-- =========================
CREATE TABLE IF NOT EXISTS DataInfo (
    info_id INTEGER PRIMARY KEY AUTOINCREMENT,
    data_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT,
    FOREIGN KEY (data_id) REFERENCES MeasurementData(data_id) ON DELETE CASCADE,
    UNIQUE (data_id, key, unit) ON CONFLICT IGNORE);

CREATE INDEX IF NOT EXISTS idx_datainfo_data ON DataInfo (data_id);
CREATE INDEX IF NOT EXISTS idx_datainfo_key ON DataInfo (key);

-- =========================
-- AnalysisRuns
-- =========================
CREATE TABLE IF NOT EXISTS AnalysisRuns (
    analysis_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    analysis_type TEXT NOT NULL,   -- 'peak_detection'
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES MeasurementSessions(session_id) ON DELETE CASCADE);

CREATE INDEX IF NOT EXISTS idx_analysis_session ON AnalysisRuns (session_id);
CREATE INDEX IF NOT EXISTS idx_analysis_type ON AnalysisRuns (analysis_type);

-- =========================
-- AnalysisFeatures
-- =========================
CREATE TABLE IF NOT EXISTS AnalysisFeatures (
    feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_id INTEGER NOT NULL,
    feature_type TEXT NOT NULL,     -- 'peak', 'valley'
    feature_index INTEGER NOT NULL, -- 0,1,2...
    FOREIGN KEY (analysis_id) REFERENCES AnalysisRuns(analysis_id) ON DELETE CASCADE,
    UNIQUE (analysis_id, feature_type, feature_index) ON CONFLICT IGNORE);

CREATE INDEX IF NOT EXISTS idx_feature_analysis ON AnalysisFeatures (analysis_id);
CREATE INDEX IF NOT EXISTS idx_feature_type ON AnalysisFeatures (feature_type);

-- =========================
-- FeatureValues
-- =========================
CREATE TABLE IF NOT EXISTS FeatureValues (
    value_id INTEGER PRIMARY KEY AUTOINCREMENT,
    feature_id INTEGER NOT NULL,
    key TEXT NOT NULL,      -- 'wavelength', 'intensity', 'fwhm'
    value REAL NOT NULL,
    unit TEXT,

    FOREIGN KEY (feature_id) REFERENCES AnalysisFeatures(feature_id) ON DELETE CASCADE,
    UNIQUE (feature_id, key, unit) ON CONFLICT IGNORE);

CREATE INDEX IF NOT EXISTS idx_value_feature ON FeatureValues (feature_id);
CREATE INDEX IF NOT EXISTS idx_value_feature_key ON FeatureValues (feature_id, key);
CREATE INDEX IF NOT EXISTS idx_value_key ON FeatureValues (key);
CREATE INDEX IF NOT EXISTS idx_value_key_value ON FeatureValues (key, value);
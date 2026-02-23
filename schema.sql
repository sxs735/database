PRAGMA foreign_keys = ON;

-- =========================
-- DUT
-- =========================
CREATE TABLE IF NOT EXISTS DUT (
    DUT_id INTEGER PRIMARY KEY,
    wafer TEXT NOT NULL,
    DOE TEXT NOT NULL,
    die INTEGER NOT NULL,
    cage TEXT NOT NULL,
    device TEXT NOT NULL,
    UNIQUE (wafer, DOE, die, cage, device));

CREATE INDEX IF NOT EXISTS idx_dut_wafer_die ON DUT (wafer, die);

-- =========================
-- Measurement
-- =========================
CREATE TABLE IF NOT EXISTS Measurement (
    measure_id INTEGER PRIMARY KEY,
    DUT_id INTEGER NOT NULL,
    measure_name TEXT,
    measured_at DATETIME,
    operator TEXT,
    system TEXT,
    notes TEXT,
    FOREIGN KEY (DUT_id) REFERENCES DUT(DUT_id) ON DELETE CASCADE,
    UNIQUE (DUT_id,measure_name));

CREATE INDEX IF NOT EXISTS idx_measure_dut ON Measurement (DUT_id);

-- =========================
-- Conditions
-- =========================
CREATE TABLE IF NOT EXISTS Conditions (
    condition_id INTEGER PRIMARY KEY,
    measure_id INTEGER NOT NULL,
    setting_parameters TEXT NOT NULL,
    setting_value REAL NOT NULL,
    parameters_unit TEXT,
    FOREIGN KEY (measure_id) REFERENCES Measurement(measure_id) ON DELETE CASCADE,
    UNIQUE (measure_id, setting_parameters, parameters_unit));

CREATE INDEX IF NOT EXISTS idx_condition_measure ON Conditions (measure_id);

-- =========================
-- MeasureSession
-- =========================
CREATE TABLE IF NOT EXISTS MeasureSession (
    session_id INTEGER PRIMARY KEY,
    measure_id INTEGER NOT NULL,
    session_idx INTEGER NOT NULL,
    UNIQUE (measure_id, session_idx),
    FOREIGN KEY (measure_id) REFERENCES Measurement(measure_id) ON DELETE CASCADE);

CREATE INDEX IF NOT EXISTS idx_session_measure ON MeasureSession (measure_id);

-- =========================
-- RawDataFiles
-- =========================
CREATE TABLE IF NOT EXISTS RawDataFiles (
    data_id INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL,
    data_type TEXT NOT NULL,     -- e.g. 'spectrum'
    file_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    recorded_at DATETIME,
    FOREIGN KEY (session_id) REFERENCES MeasureSession(session_id) ON DELETE CASCADE,
    UNIQUE (session_id, file_path));

CREATE INDEX IF NOT EXISTS idx_raw_session_type ON RawDataFiles (session_id, data_type);

-- =========================
-- DataInfo
-- =========================
CREATE TABLE IF NOT EXISTS DataInfo (
    data_id INTEGER NOT NULL,
    info_key TEXT NOT NULL,
    info_value TEXT NOT NULL,
    info_unit TEXT,
    PRIMARY KEY (data_id, info_key, info_unit),
    FOREIGN KEY (data_id) REFERENCES RawDataFiles(data_id) ON DELETE CASCADE);

CREATE INDEX IF NOT EXISTS idx_datainfo_data ON DataInfo (data_id);
CREATE INDEX IF NOT EXISTS idx_datainfo_key ON DataInfo (info_key);

-- =========================
-- Analyses
-- =========================
CREATE TABLE IF NOT EXISTS Analyses (
    analysis_id INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL,
    analysis_type TEXT NOT NULL,   -- 'peak_detection'
    instance_no INTEGER NOT NULL,
    algorithm TEXT NOT NULL,
    version TEXT NOT NULL,
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES MeasureSession(session_id) ON DELETE CASCADE,
    UNIQUE (session_id, analysis_type, instance_no));

CREATE INDEX IF NOT EXISTS idx_analysis_session ON Analyses (session_id);
CREATE INDEX IF NOT EXISTS idx_analysis_type ON Analyses (analysis_type);

-- =========================
-- AnalysisSources
-- =========================
CREATE TABLE IF NOT EXISTS AnalysisSources (
    analysis_id INTEGER NOT NULL,
    data_id INTEGER NOT NULL,
    PRIMARY KEY (analysis_id, data_id),
    FOREIGN KEY (analysis_id) REFERENCES Analyses(analysis_id) ON DELETE CASCADE,
    FOREIGN KEY (data_id) REFERENCES RawDataFiles(data_id) ON DELETE CASCADE);

CREATE INDEX IF NOT EXISTS idx_analysisinput_analysis ON AnalysisSources (analysis_id);
CREATE INDEX IF NOT EXISTS idx_analysisinput_data ON AnalysisSources (data_id);

-- =========================
-- Features
-- =========================
CREATE TABLE IF NOT EXISTS Features (
    feature_id INTEGER PRIMARY KEY,
    analysis_id INTEGER NOT NULL,
    feature_type TEXT NOT NULL,     -- 'peak', 'valley'
    feature_idx INTEGER NOT NULL, -- 0,1,2...
    FOREIGN KEY (analysis_id) REFERENCES Analyses(analysis_id) ON DELETE CASCADE,
    UNIQUE (analysis_id, feature_type, feature_idx));

CREATE INDEX IF NOT EXISTS idx_feature_analysis ON Features (analysis_id);
CREATE INDEX IF NOT EXISTS idx_feature_type ON Features (feature_type);

-- =========================
-- FeatureMetrics
-- =========================
CREATE TABLE IF NOT EXISTS FeatureMetrics (
    metric_id INTEGER PRIMARY KEY,
    feature_id INTEGER NOT NULL,
    metric_key TEXT NOT NULL,      -- 'wavelength', 'intensity', 'fwhm'
    metric_value REAL NOT NULL,
    metric_unit TEXT,

    FOREIGN KEY (feature_id) REFERENCES Features(feature_id) ON DELETE CASCADE,
    UNIQUE (feature_id, metric_key, metric_unit));

CREATE INDEX IF NOT EXISTS idx_value_feature ON FeatureMetrics (feature_id);
CREATE INDEX IF NOT EXISTS idx_value_feature_key ON FeatureMetrics (feature_id, metric_key);
CREATE INDEX IF NOT EXISTS idx_value_key ON FeatureMetrics (metric_key);
CREATE INDEX IF NOT EXISTS idx_value_key_value ON FeatureMetrics (metric_key, metric_value);
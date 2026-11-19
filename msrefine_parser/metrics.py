"""Static metadata for the 47 named MSRefine summary metrics.

Names and units follow the supplied TXT headers and HTML descriptions. ``unit``
describes the stored value; ``display_scale`` only controls presentation and must
never change the source value. ``ratio`` is dimensionless, and ``log10`` denotes
the reported logarithmic scale rather than a physical intensity unit.

``status`` records metadata review status, not a quality or alarm assessment.
The two suspect precursor columns retain their source names and values: this
catalogue does not swap them or discard the sign of any measurement.
"""


def _metric(
    code: str,
    name: str,
    group: str,
    unit: str | None,
    display_unit: str | None = None,
    display_scale: int = 1,
    value_type: str = "number",
    status: str = "mapped",
) -> dict:
    return {
        "code": code,
        "name": name,
        "group": group,
        "unit": unit,
        "display_unit": display_unit,
        "display_scale": display_scale,
        "value_type": value_type,
        "status": status,
    }


# The source header is the lookup key; spelling and punctuation are intentional.
METRIC_DEFINITIONS: dict[str, dict] = {
    "Specificity of digestion": _metric(
        "digestion_specificity", "酶切特异性肽段比例",
        "sample_preparation", "ratio", "%", 100, value_type="ratio",
    ),
    "Full cleavage": _metric(
        "full_cleavage_fraction", "无漏切位点肽段比例",
        "sample_preparation", "ratio", "%", 100, value_type="ratio",
    ),
    "Semi/full tryptic peptides": _metric(
        "semi_full_tryptic_peptide_ratio", "半酶切/全酶切肽段数量比",
        "sample_preparation", "ratio",
    ),
    "Cysteine modification": _metric(
        "cysteine_modification_fraction", "半胱氨酸修饰比例",
        "sample_preparation", "ratio", "%", 100, value_type="ratio",
    ),
    "Contaminants": _metric(
        "contaminant_psm_count", "污染物肽谱匹配数量",
        "sample_preparation", "count", "个", value_type="integer",
    ),
    "Invalid acquiring time": _metric(
        "invalid_acquiring_time", "色谱无效采集时间",
        "chromatography", "min", "min",
    ),
    "Med. FWHM": _metric(
        "median_fwhm", "半峰宽中位数",
        "chromatography", "min", "min",
    ),
    "Med. peptide eluting width": _metric(
        "median_peptide_eluting_width", "肽段洗脱宽度中位数",
        "chromatography", "min", "min",
    ),
    "Tailing ratio for IDs": _metric(
        "long_eluting_width_psm_fraction", "长洗脱宽度肽谱匹配比例",
        "chromatography", "ratio", "%", 100, value_type="ratio",
    ),
    "Scan/peptide rate": _metric(
        "scan_peptide_ratio", "平均每肽段 MS2 扫描数",
        "dynamic_sampling", "ratio",
    ),
    "Oversampling once/twice": _metric(
        "oversampling_once_twice_ratio", "一次/两次扫描鉴定前体数量比",
        "dynamic_sampling", "ratio",
    ),
    "Oversampling twice/thrice": _metric(
        "oversampling_twice_thrice_ratio", "两次/三次扫描鉴定前体数量比",
        "dynamic_sampling", "ratio",
    ),
    "Max. redundant scans": _metric(
        "max_redundant_scan_count", "单肽段最大重复扫描次数",
        "dynamic_sampling", "count", "次", value_type="integer",
    ),
    "MS1 max/sampled for IDs": _metric(
        "ms1_max_sampled_intensity_ratio", "MS1 峰顶强度/采样强度比",
        "dynamic_sampling", "ratio",
    ),
    "1+/2+": _metric(
        "charge_1_2_feature_ratio", "一价/二价肽段特征数量比",
        "ion_source", "ratio",
    ),
    "3+/2+": _metric(
        "charge_3_2_feature_ratio", "三价/二价肽段特征数量比",
        "ion_source", "ratio",
    ),
    "4+/2+": _metric(
        "charge_4_2_feature_ratio", "四价/二价肽段特征数量比",
        "ion_source", "ratio",
    ),
    "Med. precursor m/z": _metric(
        "median_precursor_mz", "已鉴定前体质荷比中位数",
        "ion_source", "m/z", "m/z",
    ),
    "Proportion of MS1 time": _metric(
        "ms1_time_fraction", "MS1 采集时间占比",
        "ms_signal", "ratio", "%", 100, value_type="ratio",
    ),
    # Neither supplied HTML description specifies an injection-time unit.
    "Med. MS1 ion injection time": _metric(
        "median_ms1_ion_injection_time", "MS1 离子注入时间中位数",
        "ms_signal", None,
    ),
    "Med. peaks number of MS1": _metric(
        "median_ms1_peak_count", "MS1 峰数中位数",
        "ms_signal", "count", "个",
    ),
    "Mean intensities of MS1": _metric(
        "mean_ms1_tic_log10", "MS1 平均总离子强度（log10）",
        "ms_signal", "log10", "log10",
    ),
    "T / D features": _metric(
        "targeted_detected_feature_fraction", "已靶向/已检测特征比例",
        "ms_signal", "ratio", "%", 100, value_type="ratio",
    ),
    "I / T features": _metric(
        "identified_targeted_feature_fraction", "已鉴定/已靶向特征比例",
        "ms_signal", "ratio", "%", 100, value_type="ratio",
    ),
    "Med. precursor-peptide m/z difference": _metric(
        "median_precursor_mass_deviation_da", "前体质量绝对偏差中位数",
        "ms_signal", "Da", "Da",
    ),
    # TXT labels disagree with the supplied HTML and detail-file statistics.
    # Preserve both raw fields; no automatic correction or sign conversion.
    "Mean ppm precursor-peptide m/z difference": _metric(
        "mean_precursor_mass_deviation_ppm",
        "前体质量偏差均值（源列名，待核对）",
        "ms_signal", "ppm", "ppm", status="needs_review",
    ),
    "Std. ppm precursor-peptide m/z difference": _metric(
        "std_precursor_mass_deviation_ppm",
        "前体质量偏差标准差（源列名，待核对）",
        "ms_signal", "ppm", "ppm", status="needs_review",
    ),
    "TopN": _metric(
        "mean_ms2_scan_count_per_cycle", "每周期平均 MS2 扫描数",
        "ms_signal", "count/cycle", "次/周期",
    ),
    "Avg. MS2 acquiring speed": _metric(
        "avg_ms2_acquiring_speed", "MS2 平均采集速度",
        "ms_signal", "Hz", "Hz",
    ),
    "Max. MS2 acquiring speed": _metric(
        "max_ms2_acquiring_speed", "MS2 最大采集速度",
        "ms_signal", "Hz", "Hz",
    ),
    "Med. MS2 ion injection time": _metric(
        "median_ms2_ion_injection_time", "MS2 离子注入时间中位数",
        "ms_signal", None,
    ),
    "Med. peaks number of MS2": _metric(
        "median_ms2_peak_count", "MS2 峰数中位数",
        "ms_signal", "count", "个",
    ),
    "Mean intensities of MS2": _metric(
        "mean_ms2_tic_log10", "MS2 平均总离子强度（log10）",
        "ms_signal", "log10", "log10",
    ),
    "MS2 ID rate": _metric(
        "ms2_id_rate", "MS2 鉴定率",
        "ms_signal", "ratio", "%", 100, value_type="ratio",
    ),
    # HTML describes scans/minute but displays a percentage (even above 100%).
    # Keep this ambiguous field unscaled, without guessing its unit or formula.
    "Identified MS2 over RT": _metric(
        "identified_ms2_over_rt", "保留时间维度的已鉴定 MS2 指标",
        "ms_signal", None, status="needs_review",
    ),
    "Max. ID rate over topN": _metric(
        "max_id_rate_over_topn", "TopN 维度最大鉴定率",
        "ms_signal", "ratio", "%", 100,
    ),
    "Mean ppm fragment m/z difference": _metric(
        "mean_fragment_mass_deviation_ppm", "碎片质量偏差均值",
        "ms_signal", "ppm", "ppm",
    ),
    "Std. ppm fragment m/z difference": _metric(
        "std_fragment_mass_deviation_ppm", "碎片质量偏差标准差",
        "ms_signal", "ppm", "ppm",
    ),
    "Dynamic range": _metric(
        "identified_feature_dynamic_range_log10",
        "已鉴定特征强度动态范围（log10 差值）",
        "ms_signal", "log10", "log10 差值",
    ),
    # A log10 unit is not inferred from the scoring thresholds of this field.
    "Med. intensities for IDs": _metric(
        "median_identified_feature_intensity", "已鉴定特征强度中位数",
        "ms_signal", None,
    ),
    "Mean number of tags": _metric(
        "mean_sequence_tag_count", "每 MS2 扫描五肽序列标签数均值",
        "ms_signal", "count", "个",
    ),
    "Med. PIF": _metric(
        "median_precursor_ion_fraction", "前体离子占比中位数",
        "ms_signal", "ratio", "%", 100, value_type="ratio",
    ),
    "Number of PSMs": _metric(
        "psm_count", "肽谱匹配数量",
        "identification", "count", "个", value_type="integer",
    ),
    "Number of unique peptides": _metric(
        "unique_peptide_count", "唯一肽段数量（含修饰和电荷态）",
        "identification", "count", "个", value_type="integer",
    ),
    "Number of unique sequences": _metric(
        "unique_sequence_count", "唯一肽段序列数量",
        "identification", "count", "个", value_type="integer",
    ),
    "Number of protein groups": _metric(
        "protein_group_count", "蛋白组数量",
        "identification", "count", "个", value_type="integer",
    ),
    "Med. value score for IDs": _metric(
        "median_identification_fdr", "鉴定结果 FDR 中位数",
        "identification", "ratio", "%", 100, value_type="ratio",
    ),
}


PRIMARY_METRIC_CODES: tuple[str, ...] = (
    "median_fwhm",
    "median_peptide_eluting_width",
    "avg_ms2_acquiring_speed",
    "ms2_id_rate",
    "median_ms1_peak_count",
    "median_ms2_peak_count",
    "mean_fragment_mass_deviation_ppm",
    "protein_group_count",
)


UNCONFIRMED_SOURCE_NAMES: frozenset[str] = frozenset({
    "Mean ppm precursor-peptide m/z difference",
    "Std. ppm precursor-peptide m/z difference",
    "Identified MS2 over RT",
})

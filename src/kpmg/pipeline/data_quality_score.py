def calculate_data_quality_score(scores, weights):
    """
    Calculates the Data Quality Score using weighted average.
    :param scores: Dictionary with data quality dimensions and their scores (0-100)
    :param weights: Dictionary with data quality dimensions and their weights (sum should be 1)
    :return: Data Quality Score (0-100)
    """
    if sum(weights.values()) != 1:
        raise ValueError("Weights must sum to 1 (100%)")
    
    data_quality_score = sum(scores[dim] * weights[dim] for dim in scores)
    return round(data_quality_score, 2)

def calculate_accuracy(correct_records, total_records):
    return round((correct_records / total_records) * 100, 2) if total_records else 0

def calculate_completeness(non_missing_values, total_values):
    return round((non_missing_values / total_values) * 100, 2) if total_values else 0

def calculate_consistency(consistent_records, total_records):
    return round((consistent_records / total_records) * 100, 2) if total_records else 0

def calculate_timeliness(up_to_date_records, total_records):
    return round((up_to_date_records / total_records) * 100, 2) if total_records else 0

def calculate_uniqueness(unique_records, total_records):
    return round((unique_records / total_records) * 100, 2) if total_records else 0

def calculate_validity(valid_records, total_records):
    return round((valid_records / total_records) * 100, 2) if total_records else 0

# Example input
scores = {
    "Accuracy": calculate_accuracy(85, 100),
    "Completeness": calculate_completeness(90, 100),
    "Consistency": calculate_consistency(80, 100),
    "Timeliness": calculate_timeliness(70, 100),
    "Uniqueness": calculate_uniqueness(95, 100),
    "Validity": calculate_validity(88, 100),
}

weights = {
    "Accuracy": 0.30,
    "Completeness": 0.25,
    "Consistency": 0.15,
    "Timeliness": 0.10,
    "Uniqueness": 0.10,
    "Validity": 0.10,
}

# Calculate and print Data Quality Score
data_quality_score = calculate_data_quality_score(scores, weights)
print(f"Data Quality Score: {data_quality_score}/100")

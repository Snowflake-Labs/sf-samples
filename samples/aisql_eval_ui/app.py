"""
Flask backend for Snowflake AISQL Function Evaluation UI
Provides mock data and API endpoints for testing AISQL functions
"""

from flask import Flask, render_template, request, jsonify
import json
import re
from datetime import datetime
from typing import Dict, List, Any
import random

app = Flask(__name__)

# Mock data storage
golden_results = {}  # Key: execution_id
golden_datasets = {}  # Key: query hash (for reusable golden datasets)
evaluation_history = []

# Mock Snowflake tables
MOCK_TABLES = {
    "customer_reviews": [
        {"id": 1, "product": "Laptop Pro X1", "review": "Great laptop, fast performance and excellent battery life!", "sentiment": None, "category": "Electronics"},
        {"id": 2, "product": "Budget Phone", "review": "Poor quality, broke after 2 weeks. Very disappointed.", "sentiment": None, "category": "Electronics"},
        {"id": 3, "product": "Coffee Maker", "review": "Makes decent coffee but quite noisy in the morning.", "sentiment": None, "category": "Appliances"},
        {"id": 4, "product": "Running Shoes", "review": "Comfortable and lightweight, perfect for marathons!", "sentiment": None, "category": "Sports"},
        {"id": 5, "product": "Blender 3000", "review": "Terrible product, motor died within a month.", "sentiment": None, "category": "Appliances"},
        {"id": 6, "product": "Smart Watch", "review": "Amazing features and great battery life. Highly recommend!", "sentiment": None, "category": "Electronics"},
        {"id": 7, "product": "Yoga Mat", "review": "Good grip but started peeling after few uses.", "sentiment": None, "category": "Sports"},
        {"id": 8, "product": "Headphones X", "review": "Sound quality is okay for the price, nothing special.", "sentiment": None, "category": "Electronics"},
    ],
    "support_tickets": [
        {"ticket_id": 101, "description": "Cannot login to my account", "priority": None, "status": "open"},
        {"ticket_id": 102, "description": "URGENT: System is down, losing revenue!", "priority": None, "status": "open"},
        {"ticket_id": 103, "description": "How do I change my password?", "priority": None, "status": "open"},
        {"ticket_id": 104, "description": "CRITICAL: Data breach suspected, need immediate help", "priority": None, "status": "open"},
        {"ticket_id": 105, "description": "Feature request: add dark mode", "priority": None, "status": "open"},
        {"ticket_id": 106, "description": "Billing question about last invoice", "priority": None, "status": "open"},
    ],
    "product_descriptions": [
        {"product_id": 1, "name": "Laptop Pro X1", "specs": "16GB RAM, 512GB SSD, Intel i7", "summary": None},
        {"product_id": 2, "name": "Wireless Mouse", "specs": "Bluetooth 5.0, 6 buttons, ergonomic design", "summary": None},
        {"product_id": 3, "name": "4K Monitor", "specs": "27 inch, IPS panel, 144Hz refresh rate", "summary": None},
        {"product_id": 4, "name": "Mechanical Keyboard", "specs": "RGB backlight, Cherry MX switches, aluminum frame", "summary": None},
    ],
    "customer_feedback": [
        {"feedback_id": 1, "text": "Your service is excellent, keep up the good work!", "needs_response": None},
        {"feedback_id": 2, "text": "I have a question about my recent order #12345", "needs_response": None},
        {"feedback_id": 3, "text": "Thanks for the quick delivery!", "needs_response": None},
        {"feedback_id": 4, "text": "There's an issue with my payment, please contact me ASAP", "needs_response": None},
        {"feedback_id": 5, "text": "Love your products!", "needs_response": None},
    ]
}

def mock_ai_complete(prompt: str, context: str = "") -> str:
    """Mock AI_COMPLETE function"""
    responses = {
        "summarize": [
            "High-performance laptop with excellent specifications and battery life.",
            "Ergonomic wireless mouse with advanced connectivity.",
            "Professional-grade 4K monitor with high refresh rate.",
            "Premium mechanical keyboard with customizable lighting."
        ],
        "sentiment": ["positive", "negative", "neutral", "mixed"],
        "priority": ["low", "medium", "high", "critical"],
        "category": ["Electronics", "Appliances", "Sports", "Other"],
    }
    
    if "summarize" in prompt.lower() or "summary" in prompt.lower():
        return random.choice(responses["summarize"])
    elif "sentiment" in prompt.lower():
        return random.choice(responses["sentiment"])
    elif "priority" in prompt.lower():
        return random.choice(responses["priority"])
    elif "category" in prompt.lower() or "classify" in prompt.lower():
        return random.choice(responses["category"])
    else:
        return f"Mock response for: {context[:50]}..."

def mock_ai_classify(text: str, labels: List[str]) -> str:
    """Mock AI_CLASSIFY function"""
    # Simple keyword-based classification for demo
    text_lower = text.lower()
    
    if "positive" in labels or "negative" in labels or "neutral" in labels:
        # Sentiment classification
        positive_words = ["great", "excellent", "amazing", "love", "perfect", "highly recommend"]
        negative_words = ["poor", "terrible", "broke", "died", "disappointed", "issue"]
        
        if any(word in text_lower for word in positive_words):
            return "positive"
        elif any(word in text_lower for word in negative_words):
            return "negative"
        else:
            return "neutral"
    
    elif "critical" in labels or "high" in labels or "urgent" in labels:
        # Priority classification
        if "critical" in text_lower or "urgent" in text_lower or "breach" in text_lower:
            return "critical"
        elif "important" in text_lower or "asap" in text_lower:
            return "high"
        elif "question" in text_lower or "how" in text_lower:
            return "low"
        else:
            return "medium"
    
    return random.choice(labels)

def mock_ai_filter(text: str, condition: str) -> bool:
    """Mock AI_FILTER function"""
    text_lower = text.lower()
    condition_lower = condition.lower()
    
    if "response" in condition_lower or "action" in condition_lower:
        action_keywords = ["question", "issue", "problem", "help", "asap", "urgent"]
        return any(keyword in text_lower for keyword in action_keywords)
    
    elif "positive" in condition_lower:
        positive_words = ["great", "excellent", "amazing", "love", "perfect"]
        return any(word in text_lower for word in positive_words)
    
    elif "negative" in condition_lower:
        negative_words = ["poor", "terrible", "broke", "died", "disappointed"]
        return any(word in text_lower for word in negative_words)
    
    return random.choice([True, False])

def execute_mock_query(query: str, table_name: str) -> List[Dict[str, Any]]:
    """Execute mock AISQL query"""
    if table_name not in MOCK_TABLES:
        return []
    
    table_data = MOCK_TABLES[table_name].copy()
    results = []
    
    # Parse query to determine function type
    query_upper = query.upper()
    
    for row in table_data:
        result_row = row.copy()
        
        if "AI_COMPLETE" in query_upper:
            # Extract the prompt and column reference
            if "summarize" in query.lower():
                # Generate summary
                for key, value in row.items():
                    if isinstance(value, str) and len(value) > 20:
                        result_row["ai_result"] = mock_ai_complete("summarize", value)
                        break
            else:
                # Generate other completions
                context = str(list(row.values()))
                result_row["ai_result"] = mock_ai_complete(query, context)
        
        elif "AI_CLASSIFY" in query_upper:
            # Extract labels from query
            labels_match = re.search(r"ARRAY\s*\[(.*?)\]", query, re.IGNORECASE)
            if labels_match:
                labels_str = labels_match.group(1)
                labels = [l.strip().strip("'\"") for l in labels_str.split(",")]
                
                # Find text column to classify
                text_col = None
                for key, value in row.items():
                    if isinstance(value, str) and len(value) > 10:
                        text_col = value
                        break
                
                if text_col:
                    result_row["ai_result"] = mock_ai_classify(text_col, labels)
        
        elif "AI_FILTER" in query_upper:
            # Extract condition
            condition_match = re.search(r"AI_FILTER\s*\((.*?),\s*'(.*?)'\)", query, re.IGNORECASE)
            if condition_match:
                column = condition_match.group(1).strip()
                condition = condition_match.group(2).strip()
                
                # Find text to filter
                text_col = None
                for key, value in row.items():
                    if isinstance(value, str) and len(value) > 10:
                        text_col = value
                        break
                
                if text_col and mock_ai_filter(text_col, condition):
                    result_row["ai_result"] = True
                    results.append(result_row)
                continue
        
        results.append(result_row)
    
    return results

def calculate_classification_metrics(predictions: List[str], golden: List[str]) -> Dict[str, float]:
    """Calculate precision, recall, F1 for classification tasks"""
    if len(predictions) != len(golden):
        return {"error": "Mismatched lengths"}
    
    # Get unique labels
    labels = list(set(predictions + golden))
    
    metrics = {}
    total_tp = 0
    total_fp = 0
    total_fn = 0
    
    for label in labels:
        tp = sum(1 for p, g in zip(predictions, golden) if p == label and g == label)
        fp = sum(1 for p, g in zip(predictions, golden) if p == label and g != label)
        fn = sum(1 for p, g in zip(predictions, golden) if p != label and g == label)
        
        total_tp += tp
        total_fp += fp
        total_fn += fn
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
        
        metrics[f"{label}_precision"] = round(precision, 3)
        metrics[f"{label}_recall"] = round(recall, 3)
        metrics[f"{label}_f1"] = round(f1, 3)
    
    # Overall metrics
    overall_precision = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0
    overall_recall = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0
    overall_f1 = 2 * overall_precision * overall_recall / (overall_precision + overall_recall) if (overall_precision + overall_recall) > 0 else 0
    
    accuracy = sum(1 for p, g in zip(predictions, golden) if p == g) / len(predictions)
    
    metrics["overall_precision"] = round(overall_precision, 3)
    metrics["overall_recall"] = round(overall_recall, 3)
    metrics["overall_f1"] = round(overall_f1, 3)
    metrics["accuracy"] = round(accuracy, 3)
    
    return metrics

def create_query_key(query: str, table_name: str) -> str:
    """Create a unique key for a query + table combination"""
    import hashlib
    normalized_query = ' '.join(query.strip().split())  # Normalize whitespace
    key_string = f"{table_name}::{normalized_query}"
    return hashlib.md5(key_string.encode()).hexdigest()[:16]

def llm_as_judge_evaluation(generated: str, reference: str, criteria: str) -> Dict[str, Any]:
    """Mock LLM as judge evaluation"""
    # Simple mock evaluation based on string similarity and length
    similarity_score = len(set(generated.lower().split()) & set(reference.lower().split())) / max(len(generated.split()), len(reference.split()), 1)
    
    length_ratio = min(len(generated), len(reference)) / max(len(generated), len(reference), 1)
    
    # Mock scores
    relevance = round(min(similarity_score * 100, 100), 1)
    coherence = round(random.uniform(70, 95), 1)
    fluency = round(random.uniform(75, 98), 1)
    overall = round((relevance + coherence + fluency) / 3, 1)
    
    feedback = []
    if relevance < 70:
        feedback.append("Generated text lacks relevance to reference")
    if len(generated) < 10:
        feedback.append("Generated text is too short")
    if len(generated) > len(reference) * 2:
        feedback.append("Generated text is overly verbose")
    if not feedback:
        feedback.append("Good quality response")
    
    return {
        "relevance": relevance,
        "coherence": coherence,
        "fluency": fluency,
        "overall_score": overall,
        "feedback": feedback
    }

# API Routes

@app.route('/')
def index():
    """Serve the main UI"""
    return render_template('index.html')

@app.route('/api/tables', methods=['GET'])
def get_tables():
    """Get list of available tables"""
    tables = []
    for table_name, data in MOCK_TABLES.items():
        columns = list(data[0].keys()) if data else []
        tables.append({
            "name": table_name,
            "columns": columns,
            "row_count": len(data)
        })
    return jsonify(tables)

@app.route('/api/table/<table_name>', methods=['GET'])
def get_table_data(table_name):
    """Get table data"""
    if table_name not in MOCK_TABLES:
        return jsonify({"error": "Table not found"}), 404
    return jsonify(MOCK_TABLES[table_name])

@app.route('/api/execute', methods=['POST'])
def execute_query():
    """Execute AISQL query"""
    data = request.json
    query = data.get('query', '')
    table_name = data.get('table_name', '')
    loaded_golden_data = data.get('loaded_golden_data')  # Optional: if golden dataset was loaded
    
    if not query or not table_name:
        return jsonify({"error": "Query and table_name required"}), 400
    
    try:
        results = execute_mock_query(query, table_name)
        
        # Store execution in history
        execution = {
            "id": len(evaluation_history) + 1,
            "timestamp": datetime.now().isoformat(),
            "query": query,
            "table_name": table_name,
            "results": results,
            "result_count": len(results)
        }
        evaluation_history.append(execution)
        
        # If a golden dataset was loaded, associate it with this execution
        if loaded_golden_data:
            golden_results[execution["id"]] = {
                "timestamp": datetime.now().isoformat(),
                "data": loaded_golden_data
            }
        
        return jsonify({
            "execution_id": execution["id"],
            "results": results,
            "row_count": len(results)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/golden', methods=['POST'])
def save_golden():
    """Save golden results"""
    data = request.json
    execution_id = data.get('execution_id')
    golden_data = data.get('golden_data')
    
    if not execution_id or not golden_data:
        return jsonify({"error": "execution_id and golden_data required"}), 400
    
    # Find the execution to get query and table name
    execution = next((e for e in evaluation_history if e["id"] == execution_id), None)
    if not execution:
        return jsonify({"error": "Execution not found"}), 404
    
    query = execution["query"]
    table_name = execution["table_name"]
    query_key = create_query_key(query, table_name)
    
    # Save by execution ID
    golden_results[execution_id] = {
        "timestamp": datetime.now().isoformat(),
        "data": golden_data
    }
    
    # Also save as reusable dataset keyed by query
    golden_datasets[query_key] = {
        "query": query,
        "table_name": table_name,
        "timestamp": datetime.now().isoformat(),
        "data": golden_data,
        "row_count": len(golden_data)
    }
    
    return jsonify({
        "message": "Golden results saved",
        "execution_id": execution_id,
        "query_key": query_key
    })

@app.route('/api/golden/<int:execution_id>', methods=['GET'])
def get_golden(execution_id):
    """Get golden results for an execution"""
    if execution_id not in golden_results:
        return jsonify({"error": "No golden results found"}), 404
    return jsonify(golden_results[execution_id])

@app.route('/api/evaluate', methods=['POST'])
def evaluate():
    """Evaluate results against golden data"""
    data = request.json
    execution_id = data.get('execution_id')
    eval_type = data.get('type', 'classification')  # classification or generative
    
    if execution_id not in golden_results:
        return jsonify({"error": "No golden results found for this execution"}), 404
    
    # Find execution
    execution = next((e for e in evaluation_history if e["id"] == execution_id), None)
    if not execution:
        return jsonify({"error": "Execution not found"}), 404
    
    golden_data = golden_results[execution_id]["data"]
    predicted_results = execution["results"]
    
    if eval_type == 'classification':
        # Extract predictions and golden labels
        predictions = [r.get("ai_result") for r in predicted_results]
        golden_labels = [g.get("ai_result") for g in golden_data]
        
        metrics = calculate_classification_metrics(predictions, golden_labels)
        
        return jsonify({
            "type": "classification",
            "metrics": metrics,
            "sample_comparisons": [
                {
                    "predicted": p,
                    "golden": g,
                    "match": p == g
                }
                for p, g in zip(predictions[:5], golden_labels[:5])
            ]
        })
    
    else:  # generative
        # LLM as judge evaluation
        evaluations = []
        for pred, gold in zip(predicted_results, golden_data):
            pred_text = pred.get("ai_result", "")
            gold_text = gold.get("ai_result", "")
            
            eval_result = llm_as_judge_evaluation(pred_text, gold_text, "quality")
            evaluations.append(eval_result)
        
        # Average metrics
        avg_metrics = {
            "avg_relevance": round(sum(e["relevance"] for e in evaluations) / len(evaluations), 1),
            "avg_coherence": round(sum(e["coherence"] for e in evaluations) / len(evaluations), 1),
            "avg_fluency": round(sum(e["fluency"] for e in evaluations) / len(evaluations), 1),
            "avg_overall": round(sum(e["overall_score"] for e in evaluations) / len(evaluations), 1)
        }
        
        return jsonify({
            "type": "generative",
            "metrics": avg_metrics,
            "sample_evaluations": evaluations[:5]
        })

@app.route('/api/golden/query', methods=['POST'])
def get_golden_by_query():
    """Get golden dataset for a specific query"""
    data = request.json
    query = data.get('query', '')
    table_name = data.get('table_name', '')
    
    if not query or not table_name:
        return jsonify({"error": "query and table_name required"}), 400
    
    query_key = create_query_key(query, table_name)
    
    if query_key not in golden_datasets:
        return jsonify({"exists": False}), 200
    
    return jsonify({
        "exists": True,
        "query_key": query_key,
        "golden_data": golden_datasets[query_key]
    })

@app.route('/api/golden/datasets', methods=['GET'])
def list_golden_datasets():
    """List all available golden datasets"""
    datasets = []
    for key, data in golden_datasets.items():
        datasets.append({
            "query_key": key,
            "query": data["query"][:100] + "..." if len(data["query"]) > 100 else data["query"],
            "full_query": data["query"],
            "table_name": data["table_name"],
            "timestamp": data["timestamp"],
            "row_count": data["row_count"]
        })
    
    # Sort by timestamp, most recent first
    datasets.sort(key=lambda x: x["timestamp"], reverse=True)
    
    return jsonify(datasets)

@app.route('/api/history', methods=['GET'])
def get_history():
    """Get evaluation history"""
    return jsonify(evaluation_history)

if __name__ == '__main__':
    app.run(debug=True, port=5000)


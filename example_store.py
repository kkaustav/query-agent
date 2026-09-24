import json
import numpy as np
from typing import List, Dict, Any

GOLDEN_EXAMPLES = [
    {
        "question": "What are the top 3 best-selling products by revenue?",
        "sql": """SELECT p.product_name, SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_revenue DESC
LIMIT 3;"""
    },
    {
        "question": "Show total spending and order count per customer in Europe.",
        "sql": """SELECT c.name, c.email, COUNT(o.order_id) AS total_orders, SUM(o.total_amount) AS total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE c.region = 'Europe'
GROUP BY c.customer_id, c.name, c.email;"""
    },
    {
        "question": "Which orders were completed in February 2024?",
        "sql": """SELECT order_id, customer_id, order_date, total_amount
FROM orders
WHERE status = 'Completed'
  AND strftime('%Y-%m', order_date) = '2024-02';"""
    },
    {
        "question": "Find products whose price is above the average product price.",
        "sql": """SELECT product_name, category, price
FROM products
WHERE price > (SELECT AVG(price) FROM products);"""
    },
    {
        "question": "List each customer's most recent order.",
        "sql": """SELECT c.name, o.order_id, o.order_date, o.total_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date = (
    SELECT MAX(o2.order_date)
    FROM orders o2
    WHERE o2.customer_id = c.customer_id
);"""
    }
]

class ExampleStore:
    def __init__(self, bedrock_client, model: str = "amazon.titan-embed-text-v2:0"):
        self.client = bedrock_client
        self.model = model
        self.examples = GOLDEN_EXAMPLES
        self.example_embeddings: np.ndarray = None
        self._initialize_embeddings()

    def _get_embedding(self, text: str) -> np.ndarray:
        body = json.dumps({"inputText": text.replace("\n", " ")})
        response = self.client.invoke_model(
            modelId=self.model,
            body=body,
            contentType="application/json",
            accept="application/json"
        )
        response_body = json.loads(response["body"].read())
        return np.array(response_body["embedding"], dtype=np.float32)

    def _initialize_embeddings(self):
        vectors = [self._get_embedding(ex["question"]) for ex in self.examples]
        matrix = np.array(vectors, dtype=np.float32)
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        self.example_embeddings = matrix / norms

    def retrieve(self, query: str, top_k: int = 2) -> List[Dict[str, Any]]:
        query_vec = self._get_embedding(query)
        query_norm = query_vec / np.linalg.norm(query_vec)
        similarities = np.dot(self.example_embeddings, query_norm)
        top_indices = np.argsort(similarities)[::-1][:top_k]

        results = []
        for idx in top_indices:
            results.append({
                "question": self.examples[idx]["question"],
                "sql": self.examples[idx]["sql"],
                "score": float(similarities[idx])
            })
        return results
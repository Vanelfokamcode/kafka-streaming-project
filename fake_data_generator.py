from faker import Faker
import random
from datetime import datetime

fake = Faker()

# Liste de produits réalistes
PRODUCTS = [
    {'id': 'prod_001', 'name': 'iPhone 15 Pro', 'category': 'electronics', 'price': 999},
    {'id': 'prod_002', 'name': 'MacBook Air M2', 'category': 'electronics', 'price': 1199},
    {'id': 'prod_003', 'name': 'AirPods Pro', 'category': 'electronics', 'price': 249},
    {'id': 'prod_004', 'name': 'Nike Air Max', 'category': 'fashion', 'price': 150},
    {'id': 'prod_005', 'name': 'Adidas Ultra Boost', 'category': 'fashion', 'price': 180},
    {'id': 'prod_006', 'name': 'The Psychology of Money', 'category': 'books', 'price': 15},
    {'id': 'prod_007', 'name': 'Atomic Habits', 'category': 'books', 'price': 18},
    {'id': 'prod_008', 'name': 'Sony WH-1000XM5', 'category': 'electronics', 'price': 399},
    {'id': 'prod_009', 'name': 'Levi\'s 501 Jeans', 'category': 'fashion', 'price': 89},
    {'id': 'prod_010', 'name': 'Instant Pot Duo', 'category': 'home', 'price': 99}
]

EVENT_TYPES = ['view', 'add_to_cart', 'purchase', 'remove_from_cart']

# Poids pour simuler comportement réel
EVENT_WEIGHTS = [0.70, 0.20, 0.05, 0.05]  # 70% views, 20% add_to_cart, etc.

def generate_user_click():
    """Génère un event click réaliste"""
    
    product = random.choice(PRODUCTS)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    
    event = {
        'event_id': fake.uuid4(),
        'user_id': f"user_{random.randint(1, 1000)}",  # 1000 users différents
        'session_id': fake.uuid4()[:8],
        'product_id': product['id'],
        'product_name': product['name'],
        'product_category': product['category'],
        'product_price': product['price'],
        'event_type': event_type,
        'timestamp': datetime.now().isoformat(),
        'device': random.choice(['mobile', 'desktop', 'tablet']),
        'country': random.choice(['FR', 'US', 'UK', 'DE', 'ES'])
    }
    
    return event

def generate_batch(n=100):
    """Génère N events"""
    return [generate_user_click() for _ in range(n)]

# À la fin du fichier, ajoute :
if __name__ == "__main__":
    # Test
    event = generate_user_click()
    print("📦 Event généré:")
    print(event)

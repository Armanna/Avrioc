import unittest
from src.data_generator import InteractionGenerator, DataGenerationController


class TestInteractionGenerator(unittest.TestCase):
    
    def setUp(self):
        self.generator = InteractionGenerator(
            user_count=10,
            item_count=5,
            interaction_types=['click', 'view']
        )
        
    def test_generate_interaction(self):
        interaction = self.generator.generate_interaction()
        
        self.assertIn('user_id', interaction)
        self.assertIn('item_id', interaction)
        self.assertIn('interaction_type', interaction)
        self.assertIn('timestamp', interaction)
        
        self.assertTrue(interaction['user_id'].startswith('user_'))
        
        self.assertTrue(interaction['item_id'].startswith('item_'))
        
        self.assertIn(interaction['interaction_type'], ['click', 'view'])
        
    def test_generate_batch(self):
        """Test generating a batch of interactions."""
        batch_size = 5
        batch = self.generator.generate_batch(batch_size)
        
        self.assertEqual(len(batch), batch_size)
        
        for interaction in batch:
            self.assertIn('user_id', interaction)
            self.assertIn('item_id', interaction)
            self.assertIn('interaction_type', interaction)
            self.assertIn('timestamp', interaction)


class TestDataGenerationController(unittest.TestCase):
    
    def setUp(self):
        self.generator = InteractionGenerator(
            user_count=10,
            item_count=5
        )
        self.controller = DataGenerationController(self.generator)
        
    def test_set_generation_rate(self):
        self.controller.set_generation_rate(50.0)
        self.assertEqual(self.controller.generation_rate, 50.0)
        
        self.controller.set_generation_rate(0.01)
        self.assertEqual(self.controller.generation_rate, 0.1)


if __name__ == '__main__':
    unittest.main
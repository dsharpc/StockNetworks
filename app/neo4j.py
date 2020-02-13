from py2neo import Graph, Node, Relationship
import os
from tqdm import tqdm

NEO4J_USER = os.environ['NEO4J_USER']
NEO4J_PASSWORD = os.environ['NEO4J_PASS']
NEO4J_HOST = os.environ['NEO4J_HOST']


class NeoGraph:
    def __init__(self):
        self.g = Graph(host=NEO4J_HOST, user= NEO4J_USER, password = NEO4J_PASSWORD)

    def truncate(self):
        """Remove all nodes in the graph"""
        print("----- Truncating graph -----")
        tx = self.g.begin()
        result = tx.run('MATCH (n) DETACH DELETE n')
        tx.commit()
        return result

    def add_companies(self, df):
        print("----- Starting Add companies process -----")
        tx = self.g.begin()
        for _, x in tqdm(df.iterrows(), total = len(df)):
            if x['symbol'] != "NA":
                n = Node("Symbol", name = x['symbol'], company = x['name'], variation_coefficient= x['var_coef'])
            tx.create(n)
        tx.commit()
        self.g.run("CREATE INDEX ON :Symbol(name)")
        print("----- Add companies process complete -----")

    def create_links(self, df):
        print("----- Starting relationship creation process -----")
        for _, x in tqdm(df.iterrows(), total=df.shape[0]):
            cypher = f"MATCH (s1:Symbol {{name:\'{x['symbol1']}\'}}),(s2:Symbol {{name:\'{x['symbol2']}\'}}) CREATE (s1)-[:CORR {{corr : {x['cor']}, id : '{x['id']}'}}]->(s2)"
            self.g.run(cypher)
        print("-----Relationship creation process complete -----")





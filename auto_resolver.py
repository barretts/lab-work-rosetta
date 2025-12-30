#!/usr/bin/env python3
"""
Clinical Rosetta Stone - Auto Resolver
Intelligently resolves unknown LIS shorthands without manual intervention.

Strategies:
1. Exact match from known mappings
2. Fuzzy string matching against known shorthands
3. NCI Thesaurus synonym lookup
4. Component-based LOINC search
5. Learning mode: saves successful resolutions for future use
"""

import sqlite3
import re
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

DB_PATH = Path(__file__).parent / "clinical_rosetta.db"


@dataclass
class Resolution:
    """A resolved mapping result."""
    input_text: str
    loinc_code: str
    loinc_name: str
    confidence: float  # 0.0 to 1.0
    method: str  # exact, fuzzy, synonym, component, learned
    matched_term: str


class AutoResolver:
    """
    Automatically resolves LIS shorthands to LOINC codes using multiple strategies.
    Can learn from user confirmations to improve over time.
    """
    
    # Common abbreviation expansions
    EXPANSIONS = {
        'auto': 'automated',
        'abs': 'absolute',
        'calc': 'calculated',
        'scr': 'screen',
        'scrn': 'screen',
        'lvl': 'level',
        'tot': 'total',
        'dir': 'direct',
        'ind': 'indirect',
        'est': 'esterase',
        'phos': 'phosphatase',
        'bili': 'bilirubin',
        'creat': 'creatinine',
        'gluc': 'glucose',
        'chol': 'cholesterol',
        'trig': 'triglycerides',
        'hgb': 'hemoglobin',
        'hct': 'hematocrit',
        'plt': 'platelet',
        'wbc': 'white blood cell',
        'rbc': 'red blood cell',
        'neut': 'neutrophil',
        'lymph': 'lymphocyte',
        'mono': 'monocyte',
        'eos': 'eosinophil',
        'baso': 'basophil',
        'seg': 'segmented',
        'imm': 'immature',
        'gran': 'granulocyte',
        'ua': 'urine',
        'ur': 'urine',
        'ser': 'serum',
        'plas': 'plasma',
        'spec': 'specific',
        'grav': 'gravity',
        'sg': 'specific gravity',
        'le': 'leukocyte esterase',
        'alb': 'albumin',
        'glob': 'globulin',
        'alk': 'alkaline',
        'tp': 'total protein',
        'tbili': 'total bilirubin',
        'dbili': 'direct bilirubin',
        'ast': 'aspartate aminotransferase',
        'alt': 'alanine aminotransferase',
        'ggt': 'gamma glutamyl transferase',
        'ldh': 'lactate dehydrogenase',
        'ck': 'creatine kinase',
        'cpk': 'creatine kinase',
        'bnp': 'natriuretic peptide',
        'tsh': 'thyroid stimulating hormone',
        'ft4': 'free t4',
        'ft3': 'free t3',
        'psa': 'prostate specific antigen',
        'hba1c': 'hemoglobin a1c',
        'a1c': 'hemoglobin a1c',
        'egfr': 'glomerular filtration rate',
        'gfr': 'glomerular filtration rate',
        'inr': 'international normalized ratio',
        'pt': 'prothrombin time',
        'ptt': 'partial thromboplastin time',
        'aptt': 'activated partial thromboplastin time',
        'esr': 'erythrocyte sedimentation rate',
        'crp': 'c-reactive protein',
        'rf': 'rheumatoid factor',
        'ana': 'antinuclear antibody',
        'hiv': 'human immunodeficiency virus',
        'hep': 'hepatitis',
        'hbsag': 'hepatitis b surface antigen',
        'thc': 'cannabinoid',
        'pcp': 'phencyclidine',
        'amph': 'amphetamine',
        'barb': 'barbiturate',
        'benzo': 'benzodiazepine',
        'benzodia': 'benzodiazepine',
        'etoh': 'ethanol',
    }
    
    def __init__(self, db_path: str = None, auto_learn: bool = True):
        self.db_path = db_path or str(DB_PATH)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.auto_learn = auto_learn
        self._ensure_learning_table()
    
    def _ensure_learning_table(self):
        """Create table for learned mappings if it doesn't exist."""
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS learned_mapping (
                id INTEGER PRIMARY KEY,
                input_text TEXT NOT NULL,
                normalized_text TEXT NOT NULL,
                loinc_code TEXT NOT NULL,
                confidence REAL DEFAULT 0.8,
                times_confirmed INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(normalized_text, loinc_code)
            )
        ''')
        self.conn.commit()
    
    def close(self):
        if self.conn:
            self.conn.close()
    
    def normalize(self, text: str) -> str:
        """Normalize text for matching."""
        # Lowercase
        text = text.lower().strip()
        # Remove punctuation except hyphens
        text = re.sub(r'[,\.\?\!\:\;\(\)\[\]\"\']+', ' ', text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    def expand_abbreviations(self, text: str) -> str:
        """Expand common abbreviations."""
        words = text.split()
        expanded = []
        for word in words:
            expanded.append(self.EXPANSIONS.get(word, word))
        return ' '.join(expanded)
    
    def similarity(self, a: str, b: str) -> float:
        """Calculate string similarity (0-1)."""
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()
    
    def resolve(self, text: str, min_confidence: float = 0.5) -> Optional[Resolution]:
        """
        Resolve a LIS shorthand to LOINC code using multiple strategies.
        Returns the best match above min_confidence, or None.
        """
        normalized = self.normalize(text)
        expanded = self.expand_abbreviations(normalized)
        
        candidates = []
        
        # Strategy 1: Exact match from lis_mapping
        result = self._try_exact_match(text)
        if result:
            return result
        
        # Strategy 2: Check learned mappings
        result = self._try_learned(normalized)
        if result and result.confidence >= min_confidence:
            return result
        
        # Strategy 3: Fuzzy match against known shorthands
        candidates.extend(self._try_fuzzy_match(normalized, expanded))
        
        # Strategy 4: Search LOINC synonyms (161k+ from LOINC 2.81)
        candidates.extend(self._try_synonym_search(expanded))
        
        # Strategy 5: Search NCI Thesaurus synonyms
        candidates.extend(self._try_nci_synonyms(expanded))
        
        # Strategy 6: Component-based LOINC search
        candidates.extend(self._try_component_search(expanded))
        
        # Return best candidate above threshold
        if candidates:
            candidates.sort(key=lambda x: x.confidence, reverse=True)
            best = candidates[0]
            if best.confidence >= min_confidence:
                # Auto-learn if enabled
                if self.auto_learn and best.confidence >= 0.7:
                    self._learn(text, normalized, best.loinc_code, best.confidence)
                return best
        
        return None
    
    def _try_exact_match(self, text: str) -> Optional[Resolution]:
        """Try exact match against lis_mapping table."""
        cursor = self.conn.execute('''
            SELECT m.source_code, m.target_loinc, l.long_common_name
            FROM lis_mapping m
            JOIN loinc_concept l ON m.target_loinc = l.loinc_code
            WHERE LOWER(m.source_code) = LOWER(?)
        ''', (text,))
        row = cursor.fetchone()
        if row:
            return Resolution(
                input_text=text,
                loinc_code=row['target_loinc'],
                loinc_name=row['long_common_name'],
                confidence=1.0,
                method='exact',
                matched_term=row['source_code']
            )
        return None
    
    def _try_learned(self, normalized: str) -> Optional[Resolution]:
        """Try match against learned mappings."""
        cursor = self.conn.execute('''
            SELECT lm.loinc_code, lm.confidence, lm.times_confirmed, l.long_common_name
            FROM learned_mapping lm
            JOIN loinc_concept l ON lm.loinc_code = l.loinc_code
            WHERE lm.normalized_text = ?
            ORDER BY lm.times_confirmed DESC, lm.confidence DESC
            LIMIT 1
        ''', (normalized,))
        row = cursor.fetchone()
        if row:
            # Update last_used
            self.conn.execute(
                'UPDATE learned_mapping SET last_used = CURRENT_TIMESTAMP WHERE normalized_text = ?',
                (normalized,)
            )
            self.conn.commit()
            return Resolution(
                input_text=normalized,
                loinc_code=row['loinc_code'],
                loinc_name=row['long_common_name'],
                confidence=min(row['confidence'] + (row['times_confirmed'] * 0.02), 0.99),
                method='learned',
                matched_term=normalized
            )
        return None
    
    def _try_fuzzy_match(self, normalized: str, expanded: str) -> List[Resolution]:
        """Fuzzy match against known shorthands."""
        candidates = []
        
        cursor = self.conn.execute('''
            SELECT m.source_code, m.target_loinc, l.long_common_name, m.source_description
            FROM lis_mapping m
            JOIN loinc_concept l ON m.target_loinc = l.loinc_code
        ''')
        
        for row in cursor.fetchall():
            source_norm = self.normalize(row['source_code'])
            source_exp = self.expand_abbreviations(source_norm)
            
            # Calculate similarity
            sim1 = self.similarity(normalized, source_norm)
            sim2 = self.similarity(expanded, source_exp)
            sim3 = self.similarity(expanded, self.normalize(row['long_common_name']))
            
            best_sim = max(sim1, sim2, sim3)
            
            if best_sim >= 0.6:
                candidates.append(Resolution(
                    input_text=normalized,
                    loinc_code=row['target_loinc'],
                    loinc_name=row['long_common_name'],
                    confidence=best_sim * 0.9,  # Slightly penalize fuzzy
                    method='fuzzy',
                    matched_term=row['source_code']
                ))
        
        return candidates
    
    def _try_nci_synonyms(self, expanded: str) -> List[Resolution]:
        """Search NCI Thesaurus for matching terms."""
        candidates = []
        
        # Search for terms containing our expanded text
        cursor = self.conn.execute('''
            SELECT DISTINCT n.nci_code, n.preferred_name, n.synonyms
            FROM nci_concept n
            WHERE LOWER(n.preferred_name) LIKE LOWER(?)
               OR LOWER(n.synonyms) LIKE LOWER(?)
            LIMIT 20
        ''', (f'%{expanded}%', f'%{expanded}%'))
        
        for row in cursor.fetchall():
            # Try to find a LOINC code that matches this concept
            loinc_cursor = self.conn.execute('''
                SELECT loinc_code, long_common_name
                FROM loinc_concept
                WHERE LOWER(long_common_name) LIKE LOWER(?)
                   OR LOWER(component) LIKE LOWER(?)
                LIMIT 1
            ''', (f'%{row["preferred_name"]}%', f'%{row["preferred_name"]}%'))
            
            loinc_row = loinc_cursor.fetchone()
            if loinc_row:
                sim = self.similarity(expanded, row['preferred_name'].lower())
                candidates.append(Resolution(
                    input_text=expanded,
                    loinc_code=loinc_row['loinc_code'],
                    loinc_name=loinc_row['long_common_name'],
                    confidence=sim * 0.75,  # Penalize indirect match
                    method='synonym',
                    matched_term=row['preferred_name']
                ))
        
        return candidates
    
    def _try_synonym_search(self, expanded: str) -> List[Resolution]:
        """Search concept_synonym table for matches."""
        candidates = []
        
        cursor = self.conn.execute('''
            SELECT DISTINCT s.loinc_code, s.synonym_text, l.long_common_name
            FROM concept_synonym s
            JOIN loinc_concept l ON s.loinc_code = l.loinc_code
            WHERE LOWER(s.synonym_text) LIKE LOWER(?)
            LIMIT 20
        ''', (f'%{expanded}%',))
        
        for row in cursor.fetchall():
            sim = self.similarity(expanded, row['synonym_text'].lower())
            if sim >= 0.5:
                candidates.append(Resolution(
                    input_text=expanded,
                    loinc_code=row['loinc_code'],
                    loinc_name=row['long_common_name'],
                    confidence=sim * 0.85,
                    method='synonym',
                    matched_term=row['synonym_text']
                ))
        
        return candidates
    
    def _try_component_search(self, expanded: str) -> List[Resolution]:
        """Search LOINC by component name."""
        candidates = []
        
        cursor = self.conn.execute('''
            SELECT loinc_code, long_common_name, component, short_name
            FROM loinc_concept
            WHERE LOWER(long_common_name) LIKE LOWER(?)
               OR LOWER(component) LIKE LOWER(?)
               OR LOWER(short_name) LIKE LOWER(?)
            LIMIT 10
        ''', (f'%{expanded}%', f'%{expanded}%', f'%{expanded}%'))
        
        for row in cursor.fetchall():
            sim = max(
                self.similarity(expanded, (row['long_common_name'] or '').lower()),
                self.similarity(expanded, (row['component'] or '').lower()),
                self.similarity(expanded, (row['short_name'] or '').lower())
            )
            
            if sim >= 0.4:
                candidates.append(Resolution(
                    input_text=expanded,
                    loinc_code=row['loinc_code'],
                    loinc_name=row['long_common_name'],
                    confidence=sim * 0.8,
                    method='component',
                    matched_term=row['long_common_name']
                ))
        
        return candidates
    
    def _learn(self, original: str, normalized: str, loinc_code: str, confidence: float):
        """Save a successful resolution for future use."""
        try:
            self.conn.execute('''
                INSERT INTO learned_mapping (input_text, normalized_text, loinc_code, confidence)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(normalized_text, loinc_code) DO UPDATE SET
                    times_confirmed = times_confirmed + 1,
                    last_used = CURRENT_TIMESTAMP
            ''', (original, normalized, loinc_code, confidence))
            self.conn.commit()
        except Exception:
            pass
    
    def confirm(self, text: str, loinc_code: str):
        """User confirms a mapping - boosts confidence."""
        normalized = self.normalize(text)
        self.conn.execute('''
            INSERT INTO learned_mapping (input_text, normalized_text, loinc_code, confidence, times_confirmed)
            VALUES (?, ?, ?, 0.95, 1)
            ON CONFLICT(normalized_text, loinc_code) DO UPDATE SET
                times_confirmed = times_confirmed + 1,
                confidence = MIN(confidence + 0.05, 0.99),
                last_used = CURRENT_TIMESTAMP
        ''', (text, normalized, loinc_code))
        self.conn.commit()
    
    def reject(self, text: str, loinc_code: str):
        """User rejects a mapping - removes it from learned."""
        normalized = self.normalize(text)
        self.conn.execute('''
            DELETE FROM learned_mapping 
            WHERE normalized_text = ? AND loinc_code = ?
        ''', (normalized, loinc_code))
        self.conn.commit()
    
    def batch_resolve(self, texts: List[str], min_confidence: float = 0.5) -> Dict[str, Optional[Resolution]]:
        """Resolve multiple texts at once."""
        return {text: self.resolve(text, min_confidence) for text in texts}
    
    def stats(self) -> Dict:
        """Get resolver statistics."""
        cursor = self.conn.execute('SELECT COUNT(*) FROM lis_mapping')
        known = cursor.fetchone()[0]
        
        cursor = self.conn.execute('SELECT COUNT(*) FROM learned_mapping')
        learned = cursor.fetchone()[0]
        
        cursor = self.conn.execute('SELECT COUNT(*) FROM loinc_concept')
        loinc = cursor.fetchone()[0]
        
        return {
            'known_mappings': known,
            'learned_mappings': learned,
            'loinc_codes': loinc,
            'abbreviations': len(self.EXPANSIONS)
        }


def main():
    """Demo the auto-resolver."""
    import sys
    
    resolver = AutoResolver()
    
    if len(sys.argv) > 1:
        # Resolve command-line arguments
        for term in sys.argv[1:]:
            result = resolver.resolve(term)
            if result:
                print(f"✓ \"{term}\"")
                print(f"  → {result.loinc_code}: {result.loinc_name}")
                print(f"  Method: {result.method} (confidence: {result.confidence:.2f})")
                print(f"  Matched: \"{result.matched_term}\"")
            else:
                print(f"✗ \"{term}\" - no match found")
            print()
    else:
        # Demo with test cases
        print("AUTO-RESOLVER DEMO")
        print("=" * 60)
        
        test_cases = [
            "WBC",           # Exact match
            "white count",   # Should fuzzy match
            "Hgb A1C",       # Abbreviation expansion
            "UA Leuk",       # Partial match
            "serum sodium",  # Component search
            "liver enzymes", # May not match
            "CBC diff",      # Common term
            "metabolic panel", # Panel name
        ]
        
        stats = resolver.stats()
        print(f"Database: {stats['known_mappings']} known, {stats['learned_mappings']} learned, {stats['loinc_codes']} LOINC codes")
        print(f"Abbreviations: {stats['abbreviations']}")
        print()
        
        for term in test_cases:
            result = resolver.resolve(term)
            if result:
                print(f"✓ \"{term}\"")
                print(f"  → {result.loinc_code}: {result.loinc_name[:50]}")
                print(f"  Method: {result.method} (confidence: {result.confidence:.2f})")
            else:
                print(f"✗ \"{term}\" - no match")
            print()
    
    resolver.close()


if __name__ == '__main__':
    main()

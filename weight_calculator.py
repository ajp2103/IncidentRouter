import json
import logging
from typing import List, Dict, Tuple
from datetime import datetime, time, timedelta
from dataclasses import dataclass
from config import Config
from models import MemberData
from exceptions import WeightCalculationError

logger = logging.getLogger(__name__)

@dataclass
class MemberWeight:
    """Data class to hold member weight calculation details"""
    member_id: str
    member_name: str
    workload_score: float
    role_score: float
    availability_score: float
    final_weight: float
    calculation_details: Dict

class WeightCalculator:
    """Calculates assignment weights for members based on multiple factors"""
    
    def __init__(self):
        self.workload_weight = Config.WORKLOAD_WEIGHT
        self.role_weight = Config.ROLE_WEIGHT
        self.availability_weight = Config.AVAILABILITY_WEIGHT
        self.priority_weights = Config.PRIORITY_WEIGHTS
        self.role_multipliers = Config.ROLE_MULTIPLIERS
    
    def calculate_member_weights(self, available_members: List[MemberData], 
                               member_workloads: Dict[str, List[Dict]], 
                               incident_priority: str) -> List[MemberWeight]:
        """Calculate weights for all available members"""
        if not available_members:
            raise WeightCalculationError("No available members to calculate weights for")
        
        member_weights = []
        
        for member in available_members:
            try:
                # Calculate individual scores
                workload_score = self._calculate_workload_score(
                    member_workloads.get(member.member_id, [])
                )
                
                role_score = self._calculate_role_score(member.role, member.experience_level)
                
                availability_score = self._calculate_availability_score(member)
                
                # Calculate final weighted score
                final_weight = (
                    workload_score * self.workload_weight +
                    role_score * self.role_weight +
                    availability_score * self.availability_weight
                )
                
                # Prepare calculation details for transparency
                calculation_details = {
                    'workload_incidents': len(member_workloads.get(member.member_id, [])),
                    'workload_score_raw': workload_score,
                    'role_score_raw': role_score,
                    'availability_score_raw': availability_score,
                    'weights_applied': {
                        'workload_weight': self.workload_weight,
                        'role_weight': self.role_weight,
                        'availability_weight': self.availability_weight
                    },
                    'incident_priority': incident_priority,
                    'member_role': member.role,
                    'member_experience': member.experience_level
                }
                
                member_weight = MemberWeight(
                    member_id=member.member_id,
                    member_name=member.member_name,
                    workload_score=workload_score,
                    role_score=role_score,
                    availability_score=availability_score,
                    final_weight=final_weight,
                    calculation_details=calculation_details
                )
                
                member_weights.append(member_weight)
                
                logger.debug(f"Calculated weight for {member.member_name}: {final_weight:.4f}")
                
            except Exception as e:
                logger.error(f"Error calculating weight for member {member.member_id}: {str(e)}")
                continue
        
        if not member_weights:
            raise WeightCalculationError("Failed to calculate weights for any members")
        
        # Sort by weight descending (higher weight = better candidate)
        member_weights.sort(key=lambda x: x.final_weight, reverse=True)
        
        logger.info(f"Calculated weights for {len(member_weights)} members")
        return member_weights
    
    def _calculate_workload_score(self, current_incidents: List[Dict]) -> float:
        """Calculate workload score based on current incidents"""
        if not current_incidents:
            return 1.0  # Maximum score for no workload
        
        # Calculate weighted incident count based on priorities
        weighted_count = 0.0
        for incident in current_incidents:
            priority = incident.get('priority', '4')
            weight = self.priority_weights.get(priority, 1.0)
            weighted_count += weight
        
        # Convert to score (inverse relationship - lower workload = higher score)
        # Use exponential decay to heavily penalize high workloads
        workload_score = 1.0 / (1.0 + (weighted_count * 0.5))
        
        logger.debug(f"Workload score: {workload_score:.4f} (incidents: {len(current_incidents)}, weighted: {weighted_count:.2f})")
        return workload_score
    
    def _calculate_role_score(self, role: str, experience_level: int) -> float:
        """Calculate role-based score considering experience"""
        # Base score from role
        base_multiplier = self.role_multipliers.get(role.upper(), 1.0)
        
        # Experience bonus (up to 20% boost)
        experience_bonus = min(experience_level * 0.02, 0.2)
        
        role_score = base_multiplier * (1.0 + experience_bonus)
        
        logger.debug(f"Role score: {role_score:.4f} (role: {role}, experience: {experience_level})")
        return role_score
    
    def _calculate_availability_score(self, member: MemberData) -> float:
        """Calculate availability score based on shift timing"""
        current_time = datetime.now().time()
        current_day = datetime.now().weekday()  # 0=Monday, 6=Sunday
        
        # Check if it's weekend
        is_weekend = current_day >= 5  # Saturday or Sunday
        
        if is_weekend and not member.weekend_shift_flag:
            return 0.1  # Very low score for unavailable during weekend
        
        # Parse shift times
        try:
            shift_start = time.fromisoformat(member.shift_start)
            shift_end = time.fromisoformat(member.shift_end)
        except ValueError:
            logger.warning(f"Invalid shift time format for member {member.member_id}")
            return 0.5  # Default score for invalid time format
        
        # Check if current time is within shift
        if self._is_time_in_shift(current_time, shift_start, shift_end):
            return 1.0  # Full score for being in shift
        else:
            # Calculate how close to shift start/end
            proximity_score = self._calculate_shift_proximity(current_time, shift_start, shift_end)
            return proximity_score
    
    def _is_time_in_shift(self, current_time: time, shift_start: time, shift_end: time) -> bool:
        """Check if current time is within shift hours"""
        if shift_start <= shift_end:
            # Normal shift (doesn't cross midnight)
            return shift_start <= current_time <= shift_end
        else:
            # Night shift (crosses midnight)
            return current_time >= shift_start or current_time <= shift_end
    
    def _calculate_shift_proximity(self, current_time: time, shift_start: time, shift_end: time) -> float:
        """Calculate proximity score when outside shift hours"""
        # Convert times to minutes for calculation
        current_minutes = current_time.hour * 60 + current_time.minute
        start_minutes = shift_start.hour * 60 + shift_start.minute
        end_minutes = shift_end.hour * 60 + shift_end.minute
        
        # Handle day crossing
        if start_minutes > end_minutes:
            end_minutes += 24 * 60
            if current_minutes < start_minutes:
                current_minutes += 24 * 60
        
        # Calculate distance to nearest shift boundary
        distance_to_start = abs(current_minutes - start_minutes)
        distance_to_end = abs(current_minutes - end_minutes)
        min_distance = min(distance_to_start, distance_to_end)
        
        # Convert to score (closer = higher score)
        # Maximum penalty for being 4+ hours away
        max_distance = 4 * 60  # 4 hours in minutes
        proximity_score = max(0.1, 1.0 - (min_distance / max_distance))
        
        return proximity_score
    
    def select_best_member(self, member_weights: List[MemberWeight]) -> Tuple[MemberWeight, str]:
        """Select the best member based on calculated weights"""
        if not member_weights:
            raise WeightCalculationError("No member weights provided for selection")
        
        # The list should already be sorted by weight descending
        best_member = member_weights[0]
        
        # Generate assignment reason
        reason_parts = []
        
        if best_member.workload_score > 0.8:
            reason_parts.append("low current workload")
        elif best_member.workload_score > 0.5:
            reason_parts.append("moderate workload")
        else:
            reason_parts.append("manageable workload")
        
        if best_member.role_score > 1.2:
            reason_parts.append("high experience level")
        elif best_member.role_score > 1.0:
            reason_parts.append("suitable experience")
        
        if best_member.availability_score > 0.9:
            reason_parts.append("currently available")
        elif best_member.availability_score > 0.5:
            reason_parts.append("partially available")
        
        assignment_reason = f"Selected {best_member.member_name} due to: {', '.join(reason_parts)}. Final weight: {best_member.final_weight:.4f}"
        
        logger.info(f"Selected member: {best_member.member_name} (weight: {best_member.final_weight:.4f})")
        
        return best_member, assignment_reason
    
    def get_weights_summary(self, member_weights: List[MemberWeight]) -> Dict:
        """Get a summary of all calculated weights for logging"""
        summary = {
            'total_members_evaluated': len(member_weights),
            'weight_distribution': [],
            'selected_member': member_weights[0].member_id if member_weights else None,
            'calculation_timestamp': datetime.now().isoformat()
        }
        
        for weight in member_weights:
            summary['weight_distribution'].append({
                'member_id': weight.member_id,
                'member_name': weight.member_name,
                'final_weight': weight.final_weight,
                'workload_score': weight.workload_score,
                'role_score': weight.role_score,
                'availability_score': weight.availability_score
            })
        
        return summary

# Global weight calculator instance
weight_calculator = WeightCalculator()

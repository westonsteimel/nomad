package scheduler

import (
	"math"
	"sort"

	"github.com/hashicorp/nomad/nomad/structs"
)

// resourceDistance returns how close the resource is to the resource being asked for
// It is calculated by first computing a relative fraction and then measuring how close
// that is to zero. Lower values are closer
func resourceDistance(resource *structs.Resources, resourceAsk *structs.Resources) float64 {
	memoryCoord, cpuCoord, iopsCoord, diskMBCoord := 0.0, 0.0, 0.0, 0.0
	if resourceAsk.MemoryMB > 0 {
		memoryCoord = float64(resourceAsk.MemoryMB-resource.MemoryMB) / float64(resourceAsk.MemoryMB)
	}
	if resourceAsk.CPU > 0 {
		cpuCoord = float64(resourceAsk.CPU-resource.CPU) / float64(resourceAsk.CPU)
	}
	if resourceAsk.IOPS > 0 {
		iopsCoord = float64(resourceAsk.IOPS-resource.IOPS) / float64(resourceAsk.IOPS)
	}
	if resourceAsk.DiskMB > 0 {
		diskMBCoord = float64(resourceAsk.DiskMB-resource.DiskMB) / float64(resourceAsk.DiskMB)
	}

	originDist := math.Sqrt(
		math.Pow(memoryCoord, 2) +
			math.Pow(cpuCoord, 2) +
			math.Pow(iopsCoord, 2) +
			math.Pow(diskMBCoord, 2))
	return originDist
}

// GetPreemptibleAllocs computes a list of allocations to preempt to accommodate
// the resource asked for. Only allocs with a job priority < 10 of jobPriority are considered
// This currently does not account for static port asks
func GetPreemptibleAllocs(jobPriority int, current []*structs.Allocation, resourceAsk *structs.Resources, node *structs.Node) []*structs.Allocation {

	groupedAllocs := filterAndGroupPreemptibleAllocs(jobPriority, current)
	var bestAllocs []*structs.Allocation
	allRequirementsMet := false
	var preemptedResources *structs.Resources

	networkStanzaAllocs, met := satisfyNetworkRequirements(groupedAllocs, resourceAsk.Networks[0].MBits, node)

	// If its impossible to preempt allocs to satisfy Mbits needed, return early
	if !met {
		return nil
	}

	// Filter out any allocs already in list of allocs to preempt
	groupedAllocs = filterByList(groupedAllocs, networkStanzaAllocs)

	// Add the list of allocs that satisfy network requirements
	// Also update preemptedResources which tracks total resources preempted so far
	for _, alloc := range networkStanzaAllocs {
		if preemptedResources == nil {
			preemptedResources = alloc.Resources.Copy()
		} else {
			preemptedResources.Add(alloc.Resources)
		}
		allRequirementsMet = MeetsRequirements(preemptedResources, resourceAsk)
		bestAllocs = append(bestAllocs, alloc)
	}

	// Return now if the allocs that meet network requirements also satisfy remaining requirements
	if allRequirementsMet {
		return bestAllocs
	}

	// Keep searching remaining allocs if requirements have not been met
	for _, allocGrp := range groupedAllocs {
		for len(allocGrp.allocs) > 0 && !allRequirementsMet {
			closestAllocIndex := -1
			bestDistance := math.MaxFloat64
			// find the alloc with the closest distance
			for index, alloc := range allocGrp.allocs {
				distance := resourceDistance(alloc.Resources, resourceAsk)
				if distance < bestDistance {
					bestDistance = distance
					closestAllocIndex = index
				}
			}
			if closestAllocIndex == -1 {
				// This means no option in the current list was eligible for preemption
				// Can stop looking
				break
			}
			closestAlloc := allocGrp.allocs[closestAllocIndex]
			if preemptedResources == nil {
				preemptedResources = closestAlloc.Resources.Copy()
			} else {
				preemptedResources.Add(closestAlloc.Resources)
			}
			allRequirementsMet = MeetsRequirements(preemptedResources, resourceAsk)
			bestAllocs = append(bestAllocs, closestAlloc)
			allocGrp.allocs[closestAllocIndex] = allocGrp.allocs[len(allocGrp.allocs)-1]
			allocGrp.allocs = allocGrp.allocs[:len(allocGrp.allocs)-1]
		}
		if allRequirementsMet {
			break
		}
	}

	// Early return if all allocs examined and requirements were not met
	if !allRequirementsMet {
		return nil
	}

	// We do another pass to eliminate unnecessary preemptions
	// This filters out allocs whose resources are already covered by another alloc

	// Sort by distance reversed to surface any superset allocs first
	sort.Slice(bestAllocs, func(i, j int) bool {
		distance1 := resourceDistance(bestAllocs[i].Resources, resourceAsk)
		distance2 := resourceDistance(bestAllocs[j].Resources, resourceAsk)
		return distance1 > distance2
	})

	var filteredBestAllocs []*structs.Allocation
	// Reset aggregate preempted resources so that we can do another pass
	preemptedResources = nil
	for _, alloc := range bestAllocs {
		if preemptedResources == nil {
			preemptedResources = alloc.Resources
		} else {
			preemptedResources.Add(alloc.Resources)
		}
		filteredBestAllocs = append(filteredBestAllocs, alloc)
		requirementsMet := MeetsRequirements(preemptedResources, resourceAsk)
		if requirementsMet {
			break
		}
	}

	return filteredBestAllocs

}

// filterByList removes any allocs in the map grouped by priority that are already in the list
func filterByList(allocsGrp []*groupedAllocs, allocations []*structs.Allocation) []*groupedAllocs {
	matchSet := make(map[string]struct{})
	for _, alloc := range allocations {
		matchSet[alloc.ID] = struct{}{}
	}
	var ret []*groupedAllocs
	for _, grp := range allocsGrp {
		allocs := grp.allocs
		n := len(allocs)
		for i := 0; i < n; i++ {
			if _, ok := matchSet[allocs[i].ID]; ok {
				allocs[i], allocs[n-1] = allocs[n-1], nil
				i--
				n--
			}
		}
		allocs = allocs[:n]
		if len(allocs) > 0 {
			ret = append(ret, &groupedAllocs{grp.priority, allocs})
		}
	}
	return ret
}

// MeetsRequirements checks if the first resource meets or exceeds the second resource's requirements
// TODO network iops is pretty broken and needs to be rewritten
func MeetsRequirements(first *structs.Resources, second *structs.Resources) bool {
	if first.CPU < second.CPU {
		return false
	}
	if first.MemoryMB < second.MemoryMB {
		return false
	}
	if first.DiskMB < second.DiskMB {
		return false
	}
	if first.IOPS < second.IOPS {
		return false
	}
	if len(first.Networks) > 0 && len(second.Networks) > 0 {
		if first.Networks[0].MBits < second.Networks[0].MBits {
			return false
		}
	}
	return true
}

type groupedAllocs struct {
	priority int
	allocs   []*structs.Allocation
}

func filterAndGroupPreemptibleAllocs(jobPriority int, current []*structs.Allocation) []*groupedAllocs {
	allocsByPriority := make(map[int][]*structs.Allocation)
	for _, alloc := range current {
		// Skip ineligible allocs
		// TODO(preetha): not having the first condition broke a unit test
		// Why is alloc.Job even nil though?
		if alloc.Job == nil {
			continue
		}

		if jobPriority-alloc.Job.Priority < 10 {
			continue
		}
		grpAllocs, ok := allocsByPriority[alloc.Job.Priority]
		if !ok {
			grpAllocs = make([]*structs.Allocation, 0)
		}
		grpAllocs = append(grpAllocs, alloc)
		allocsByPriority[alloc.Job.Priority] = grpAllocs
	}

	var groupedSortedAllocs []*groupedAllocs
	for priority, allocs := range allocsByPriority {
		groupedSortedAllocs = append(groupedSortedAllocs, &groupedAllocs{
			priority: priority,
			allocs:   allocs})
	}

	sort.Slice(groupedSortedAllocs, func(i, j int) bool {
		return groupedSortedAllocs[i].priority < groupedSortedAllocs[j].priority
	})

	// Sort by priority
	return groupedSortedAllocs
}

// satisfyNetworkRequirements returns whether the given node can satisfy network requirements,
// and if any allocs can be preempted to do so. The second return param indicates whether network
// requirements were satisfied
func satisfyNetworkRequirements(groupedAllocs []*groupedAllocs, MbitsNeeded int, node *structs.Node) ([]*structs.Allocation, bool) {
	deviceToAllocs := make(map[string][]*structs.Allocation)

	// Create a map from each device to allocs sorted by priority ascending
	for _, groupedAlloc := range groupedAllocs {
		for _, alloc := range groupedAlloc.allocs {
			if len(alloc.Resources.Networks) > 0 {
				device := alloc.Resources.Networks[0].Device
				allocsForDevice := deviceToAllocs[device]
				allocsForDevice = append(allocsForDevice, alloc)
				deviceToAllocs[device] = allocsForDevice
			}
		}
	}

	// If no existing allocations use network resources, return early
	if len(deviceToAllocs) == 0 {
		return nil, true
	}

	netIdx := structs.NewNetworkIndex()
	netIdx.SetNode(node)
	defer netIdx.Release()

	var allocsToPreempt []*structs.Allocation

	met := false
	for device, currentAllocs := range deviceToAllocs {
		availableBandwidth := netIdx.AvailBandwidth[device]
		if availableBandwidth < MbitsNeeded {
			continue
		}
		usedBandwidth := 0
		allocsToPreempt = nil // Reset this because we can't preempt across a device

		// Keep appending allocs in this device till meeting bandwidth needs
		for _, alloc := range currentAllocs {
			usedBandwidth += alloc.Resources.Networks[0].MBits
			allocsToPreempt = append(allocsToPreempt, alloc)
			if usedBandwidth >= MbitsNeeded {
				met = true
				break
			}
		}

		// If we met our network requirements, no need to look for another device
		if met {
			break
		}
	}

	return allocsToPreempt, met
}

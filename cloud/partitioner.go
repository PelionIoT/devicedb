package cloud

// controller per site
// site controller attached to partition
// Partition:
//   storageDriver:
//   incorporate a log for writing to secondary nodes for this partition
//   sites:
//     - Site a controller (uses storageDriver)
//     - Site b controller (uses storageDriver)
//     - Site c controller (uses storageDriver)
def sum_net_area(SourceRealEstateBuildingNetArea_col, 
    SourceRealEstateBuildingStorageArea_col, 
    SourceRealEstateBuildingParkingArea_col):
    
    """Consolidate values to SourceRealEstateBuildingNetArea"""

    if not SourceRealEstateBuildingNetArea_col:
        SourceRealEstateBuildingNetArea_col = 0.0
    
    if not SourceRealEstateBuildingStorageArea_col:
        SourceRealEstateBuildingStorageArea_col = 0.0
    
    if not SourceRealEstateBuildingParkingArea_col:
        SourceRealEstateBuildingParkingArea_col = 0.0
    
    total = (SourceRealEstateBuildingNetArea_col +
             SourceRealEstateBuildingStorageArea_col +
             SourceRealEstateBuildingParkingArea_col)
    
    return total


def unifying_address(address_1, address_2):
    final_address = ""
    
    if address_1 and len(address_1) > 0:
        final_address = address_1

    if address_2 and len(address_2) > 0:
        if address_2 not in final_address:
            final_address = final_address + " " + address_2
    
    return final_address




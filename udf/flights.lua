function stats(touples)
  
  local function accumulator(accumulation, nextElement)
    info("current:"..tostring(accumulation).." next:"..tostring(nextElement))
    -- Example results
    -- LAX,DFW,1,3
    -- LAX,DFW,2,2
    -- LAX,DFW,3,1
    local origin = nextElement["orig"]
    local destination = nextElement["dest"]
    local orig_dest = nextElement["origdest"]
    local pax = nextElement["pax"]
    local association = nextElement["assoc"]
    if accumulation[orig_dest] == nil then
      accumulation[orig_dest] = map()
      accumulation[orig_dest]["orig"] = origin
      accumulation[orig_dest]["dest"] = destination
    end 
    if accumulation[orig_dest][association] == nil then
      accumulation[orig_dest][association] = map()
      accumulation[orig_dest][association]["pax"] = map()
    end 
    accumulation[orig_dest][association]["pax"][pax] = (accumulation[orig_dest][association]["pax"][pax] or 0) + 1
    return accumulation
  end
  
  local function pax_count_merge(a, b)
    return a + b
  end
  
  local function association_merge(a, b)
    return map.merge(a, b, pax_count_merge)
  end
  
  local function orig_dest_merge(a, b)
    return map.merge(a, b, association_merge)
  end
  
  local function reducer(this, that)
    return map.merge(this, that, orig_dest_merge)
  end
  
  return touples:aggregate(map{}, accumulator):reduce(reducer)
end
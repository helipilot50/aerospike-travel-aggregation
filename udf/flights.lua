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
    if accumulation[orig_dest] == nil then
      accumulation[orig_dest] = map()
      accumulation[orig_dest]["orig"] = origin
      accumulation[orig_dest]["dest"] = destination
      accumulation[orig_dest]["pax"] = map()
    end 
    accumulation[orig_dest]["pax"][pax] = (accumulation[orig_dest][pax] or 0) + 1
    return accumulation
  end
  
  local function element_merge(a, b)
    return a + b
  end
  
  local function orig_dest_merge(a, b)
    return map.merge(a, b, element_merge)
  end
  
  local function reducer(this, that)
    return map.merge(this, that, orig_dest_merge)
  end
  
  return touples:aggregate(map{}, accumulator):reduce(reducer)
end
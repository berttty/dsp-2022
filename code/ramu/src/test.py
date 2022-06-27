from phases.grid_generation import grid

index = 0
for ele in grid('berlin'):
    if index > 2:
        break
    print(ele)
    index = index + 1

print(index)

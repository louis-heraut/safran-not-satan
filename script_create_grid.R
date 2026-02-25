download_dir = "00_data-download"
shp_file = "SIM2.shp"
output_dir = "resources"
output_file = "grid-SIM.gpkg"

points = sf::st_read(file.path(download_dir, shp_file))
# sf::st_crs(points) = 27572
points = sf::st_transform(points, 27572)
points = sf::st_geometry(points)*100
points = sf::st_sf(geometry=points, crs=27572)
coords = sf::st_coordinates(points)

grid_list = lapply(1:nrow(coords), function(i) {
    grid = 8000
    x = coords[i,1]
    y = coords[i,2]
    sf::st_polygon(list(rbind(
            c(x-grid/2, y-grid/2),
            c(x+grid/2, y-grid/2),
            c(x+grid/2, y+grid/2),
            c(x-grid/2, y+grid/2),
            c(x-grid/2, y-grid/2)
        )))
})

grid = sf::st_sf(points,
                 geometry=sf::st_sfc(grid_list, crs=sf::st_crs(points)))

sf::st_write(points, file.path(output_dir, output_file),
             layer="points", delete_layer=TRUE)
sf::st_write(grid, file.path(output_dir, output_file),
             layer="grid-cells", delete_layer=TRUE)

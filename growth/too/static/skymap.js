(function($) {

    $.fn.skymap = function(localization, fields) {
        let $svg = this,
            that = this,
             svg = d3.select($svg[0]),
            proj = d3.geoOrthographic().precision(0.1),
            path = d3.geoPath(proj);

        this.redraw = function () {
            svg.selectAll('path').attr('d', path);

            $(svg.selectAll('path.field').nodes()).tooltip({
                html: true,
                sanitize: false,
                title: function() {
                    var d = d3.select(this).datum();
                    return `
                        <table class=field-tooltip>
                            <tr>
                                <th class=text-left>Field</th>
                                <td class=text-right>
                                    ${d.properties.telescope},
                                    ${d.properties.field_id}
                                </td>
                            </tr>
                            <tr>
                                <th class=text-left>Pos</th>
                                <td class=text-right>
                                    ${
                                        Math.trunc(d.properties.ra / 15).toString().padStart(2, '0')
                                    }<sup>h</sup>${
                                        Math.trunc(d.properties.ra % 15).toString().padStart(2, '0')
                                    }<sup>m</sup>,
                                    ${
                                        d.properties.dec >= 0 ? '+' : '-'
                                    }${
                                        Math.trunc(Math.abs(d.properties.dec)).toString().padStart(2, '0')
                                    }<sup>d</sup>${
                                        Math.trunc(Math.abs(d.properties.dec) % 1 * 60).toString().padStart(2, '0')
                                    }<sup>m</sup>
                                </td>
                            </tr>
                            <tr>
                                <th class=text-left>Depth</th>
                                <td class=text-right>
                                    ${Object.entries(d.properties.depth).map((kv => '<i>' + kv[0] + '</i>=' + kv[1].toFixed(2))).join(', ')}
                                </td>
                            </tr>
                        </table>
                    `;
                }
            });
        }

        function resize() {
            var width = $svg.width(), height = $svg.height();
            proj.scale(0.5 * width).translate([width / 2, height / 2]);
            that.redraw();
        }

        svg.append('path')
            .datum({type: 'Feature', geometry: d3.geoGraticule10()})
            .attr('class', 'graticule');

        $svg.resize(resize);
        resize();

        d3.geoZoom()
            .projection(proj)
            .onMove(that.redraw)(svg.node());

        this.localization = function(features) {
            // First feature is just the maximum a posteriori position.
            // Recenter the map on this point.
            var center = features.features.shift().geometry.coordinates;
            proj.rotate([-center[0], -center[1]]);

            svg.selectAll('path.contour').remove();
            svg.append('path')
                .datum(features)
                .attr('class', 'contour');

            that.redraw();
        }

        that.redraw();

        return this;
    };

})(jQuery);

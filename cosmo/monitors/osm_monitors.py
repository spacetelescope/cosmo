# TODO: Add OSM Shift monitor and datamodel. Draft output below after data is retrieved and put in a dataframe
# traces = []
# # figure = go.Figure()
# # idx = 0
# # for name, group in groups:
# #     group = group.sort_values('EXPSTART')
# #
# #     t = []
# #     lp = []
# #     for i, row in group.iterrows():
# #         row_t = [
#               Time(row.EXPSTART, format='mjd').datetime + datetime.timedelta(seconds=lil_t) for lil_t in row.TIME
#               ]
# #         row_lp = [f'LP{row.LIFE_ADJ}' for _ in range(len(row.TIME))]
# #         lp += row_lp
# #         t += row_t
# #
# #     traces.append(
# #         go.Scattergl(
# #             x=t,
# #             y=[item for sublist in group.SHIFT_DISP.values for item in sublist],
# #             name='-'.join([str(item) for item in name]),
# #             mode='markers',
# #             marker=dict(
# #                 cmax=19,
# #                 cmin=0,
# #                 color=list(repeat(idx, len(t))),
# #                 colorscale='Viridis'
# #             ),
# #             text=lp
# #         )
# #     )
# #     idx += 1

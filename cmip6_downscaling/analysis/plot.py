import matplotlib.pyplot as plt


def plot_time_mean(
    ds1,
    ds2,
    diff=True,
    limits_dict=None,
    ds1_name=None,
    ds2_name=None,
    title=None,
    figsize=(15, 15),
    savefig=None,
):

    vars_to_plot = list(ds1.data_vars.keys())

    fig, axarr = plt.subplots(
        ncols=3, nrows=len(vars_to_plot), figsize=figsize, sharex=True, sharey=True
    )

    # for the purposes of the plot we'll drop some metadata
    # so as to not clutter the figure
    ds2.x.attrs['long_name'] = 'x'
    ds2.y.attrs['long_name'] = 'y'

    # sometimes the default colormap assignment doesn't work
    # and we want the difference plots to always be red/blue
    # and the absolute maps to always be viridis
    cmap_diff = 'RdBu'
    cmap_abs = 'viridis'
    for i, var in enumerate(vars_to_plot):
        if var in list(limits_dict.keys()):
            ### TODO: clean this up
            (vmin, vmax) = limits_dict[var]['abs']
            (vmin_diff, vmax_diff) = limits_dict[var]['diff']
            ds1[var].plot(ax=axarr[i, 0], vmin=vmin, vmax=vmax, cmap=cmap_abs)
            ds2[var].plot(ax=axarr[i, 1], vmin=vmin, vmax=vmax, cmap=cmap_abs)
            (ds2[var] - ds1[var]).plot(
                ax=axarr[i, 2], vmin=vmin_diff, vmax=vmax_diff, cmap=cmap_diff
            )
        else:
            ds1[var].plot(ax=axarr[i, 0], cmap=cmap_abs)
            ds2[var].plot(ax=axarr[i, 1], cmap=cmap_abs)
            (ds2[var] - ds1[var]).plot(ax=axarr[i, 2], cmap=cmap_diff)
    if ds1_name and ds2_name:
        column_titles = [ds1_name, ds2_name, 'Difference']
        for ax, col_name in zip(axarr[0], column_titles):
            ax.set_title(col_name, fontsize=18)
    if title:
        fig.suptitle(title, fontsize=20)
    fig.tight_layout(rect=[0, 0.03, 1, 0.98])
    if savefig:
        fig.savefig(savefig)
    return fig, axarr


def plot_seasonal_mean(ds_list, limits_dict=None):

    vars_to_plot = list(ds_list[0].data_vars.keys())

    fig, axarr = plt.subplots(nrows=len(vars_to_plot))
    for ds in ds_list:
        for i, var in enumerate(vars_to_plot):
            if var in list(limits_dict.keys()):
                vmin = limits_dict[var]['vmin']
                vmax = limits_dict[var]['vmax']
            else:
                vmin, vmax = None, None
            ds[var].plot(ax=axarr[i])
            ds[var].plot(ax=axarr[i])
            axarr[i].ylim(vmin, vmax)

    return fig, axarr
